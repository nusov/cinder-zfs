# Copyright (c) 2018 NFV Express
# Copyright (c) 2018 OpenStack Foundation
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

"""
Driver for servers running ZFS.

"""

import math
import os
import socket

from oslo_concurrency import processutils
from oslo_config import cfg
from oslo_log import log as logging
from oslo_utils import excutils
from oslo_utils import importutils
from oslo_utils import units
from oslo_utils import strutils
import six

from cinder import exception
from cinder.i18n import _
from cinder.image import image_utils
from cinder import interface
from cinder import objects
from cinder import utils
from cinder.volume import driver
from cinder.volume import volume_utils as volutils

LOG = logging.getLogger(__name__)

volume_opts = [
    cfg.StrOpt('zfs_zpool',
               default='cinder-volumes',
               help='Name for the ZFS Pool that will contain exported volumes'),
    cfg.StrOpt('zfs_type',
               default='default',
               choices=['default', 'thin'],
               help='Type of ZFS volumes to deploy; (default, thin). ')
]

CONF = cfg.CONF
CONF.register_opts(volume_opts)


@interface.volumedriver
class ZFSVolumeDriver(driver.VolumeDriver):
    """Executes commands relating to Volumes."""

    VERSION = '3.0.0'

    # ThirdPartySystems wiki page
    CI_WIKI_NAME = "Cinder_Jenkins"

    def __init__(self, *args, **kwargs):
        # Parent sets db, host, _execute and base config
        super(ZFSVolumeDriver, self).__init__(*args, **kwargs)

        self.configuration.append_config_values(volume_opts)
        self.hostname = socket.gethostname()
        self.zpool = None
        self.backend_name = \
            self.configuration.safe_get('volume_backend_name') or 'ZFS'

        # Target Driver is what handles data-transport
        # Transport specific code should NOT be in
        # the driver (control path), this way
        # different target drivers can be added (iscsi, FC etc)
        target_driver = \
            self.target_mapping[self.configuration.safe_get('target_helper')]

        LOG.debug('Attempting to initialize ZFS driver with the '
                  'following target_driver: %s',
                  target_driver)

        self.target_driver = importutils.import_object(
            target_driver,
            configuration=self.configuration,
            db=self.db,
            executor=self._execute)
        self.protocol = self.target_driver.protocol

    def _zfs_volume(self, volume, zpool=None):
        if zpool is None:
            zpool = self.configuration.zfs_zpool
        return "%s/%s" % (zpool, volume['name'])

    def _zfs_snapshot(self, snapshot, zpool=None):
        if zpool is None:
            zpool = self.configuration.zfs_zpool
        return "%s/%s@%s" % (zpool, 
                             snapshot['volume_name'], 
                             snapshot['name'])

    def _sizestr(self, size_in_g):
        return '%sG' % size_in_g

    def _fromsizestr(self, sizestr):
        return strutils.string_to_bytes(sizestr + 'B')

    def _volume_not_present(self, volume):
        root_helper = utils.get_root_helper()
        zvol = self._zfs_volume(volume)
        try:
            self._execute('zfs', 'list', zvol,
                          root_helper=root_helper,
                          run_as_root=True)
        except processutils.ProcessExecutionError:
            LOG.error(_('Unable to retrive zvol '
                        'for volume: %s'), volume['name'])
            return True
        return False

    def _update_volume_stats(self):
        """Retrieve stats info from zpool"""

        LOG.debug("Updating volume stats")
        if self.zpool is None:
            LOG.warning(_('Unable to update stats on non-initialized '
                          'ZFS Pool: %s'),
                        self.configuration.zfs_zpool)
            return

        data = {}

        root_helper = utils.get_root_helper()
        try:
            out, err = self._execute('zpool', 'list', '-H',
                                     '-oname,size,alloc,free',
                                     self.zpool,
                                     root_helper=root_helper, run_as_root=True)
            name, size, alloc, free = out.strip().split('\t')
        except processutils.ProcessExecutionError as exc:
            exception_message = (_("Failed to retrieve zvol stats, "
                                    "error message was: %s")
                                    % six.text_type(exc.stderr))
            raise exception.VolumeBackendAPIException(
                data=exception_message)

        # Note(zhiteng): These information are driver/backend specific,
        # each driver may define these values in its own config options
        # or fetch from driver specific configuration file.
        data["volume_backend_name"] = self.backend_name
        data["vendor_name"] = 'Open Source'
        data["driver_version"] = self.VERSION
        data["storage_protocol"] = self.protocol
        data["pools"] = []

        total_capacity = self._fromsizestr(size) / units.Gi
        free_capacity = self._fromsizestr(free) / units.Gi
        provisioned_capacity = self._fromsizestr(alloc) / units.Gi

        location_info = \
            ('ZFSVolumeDriver:%(hostname)s:%(zfs_zpool)s'
             ':%(zfs_type)s' %
             {'hostname': self.hostname,
              'zfs_zpool': self.configuration.zfs_zpool,
              'zfs_type': self.configuration.zfs_type})

        thin_enabled = self.configuration.zfs_type == 'thin'

        # Calculate the total volumes used by the ZFS pool.
        try:
            out, err = self._execute('zfs', 'list', '-r', '-H', 
                                     self.zpool,
                                     root_helper=root_helper,
                                     run_as_root=True)
            total_volumes = len(out.splitlines())
        except processutils.ProcessExecutionError as exc:
            exception_message = (_("Failed to retrieve count of zvols, "
                                    "error message was: %s")
                                    % six.text_type(exc.stderr))
            raise exception.VolumeBackendAPIException(
                data=exception_message)
        

        # Calculate the total snapshots used by the ZFS pool.
        try:
            out, err = self._execute('zfs', 'list', '-r', '-H',
                                     '-t', 'snapshot',
                                     self.zpool,
                                     root_helper=root_helper,
                                     run_as_root=True)
            total_snapshots = len(out.splitlines())
        except processutils.ProcessExecutionError as exc:
            exception_message = (_("Failed to retrieve count of snapshots, "
                                    "error message was: %s")
                                    % six.text_type(exc.stderr))
            raise exception.VolumeBackendAPIException(
                data=exception_message)

        # Skip enabled_pools setting, treat the whole backend as one pool
        # XXX FIXME if multipool support is added to ZFS driver.
        single_pool = {}
        single_pool.update(dict(
            pool_name=data["volume_backend_name"],
            total_capacity_gb=total_capacity,
            free_capacity_gb=free_capacity,
            reserved_percentage=self.configuration.reserved_percentage,
            location_info=location_info,
            QoS_support=False,
            provisioned_capacity_gb=provisioned_capacity,
            max_over_subscription_ratio=(
                self.configuration.max_over_subscription_ratio),
            thin_provisioning_support=thin_enabled,
            thick_provisioning_support=not thin_enabled,
            total_volumes=total_volumes,
            total_snapshots=total_snapshots,
            filter_function=self.get_filter_function(),
            goodness_function=self.get_goodness_function(),
            multiattach=True
        ))
        data["pools"].append(single_pool)
        data['sparse_copy_volume'] = True

        self._stats = data

    def check_for_setup_error(self):
        """Verify that requirements are in place to use ZFS driver."""
        root_helper = utils.get_root_helper()

        try:
            self._execute('zfs', 'list',
                          root_helper=root_helper, run_as_root=True)
        except processutils.ProcessExecutionError as exc:
            exception_message = (_("Failed to initialize ZFS driver, "
                                    "error message was: %s")
                                    % six.text_type(exc.stderr))
            raise exception.VolumeBackendAPIException(
                data=exception_message)

        try:
            self._execute('zfs', 'list', self.configuration.zfs_zpool,
                          root_helper=root_helper, run_as_root=True)
        except processutils.ProcessExecutionError as exc:
            exception_message = (_("Failed to to initilize ZFS driver, "
                                    "error message was: %s")
                                    % six.text_type(exc.stderr))
            raise exception.VolumeBackendAPIException(
                data=exception_message)

        self.zpool = self.configuration.zfs_zpool

    def create_volume(self, volume, zpool=None):
        """Creates a logical volume."""
        root_helper = utils.get_root_helper()

        cmd = ['zfs', 'create', '-V', self._sizestr(volume['size'])]
        if self.configuration.zfs_type == "thin":
            cmd.append('-s')
        cmd.append(self._zfs_volume(volume, zpool=zpool))

        try:
            self._execute(*cmd, root_helper=root_helper, run_as_root=True)
        except processutils.ProcessExecutionError as exc:
            exception_message = (_("Failed to create volume, "
                                    "error message was: %s")
                                    % six.text_type(exc.stderr))
            raise exception.VolumeBackendAPIException(
                data=exception_message)
        
    def update_migrated_volume(self, ctxt, volume, new_volume,
                               original_volume_status):
        """Return model update from ZFS for migrated volume.

        This method should rename the back-end volume name(id) on the
        destination host back to its original name(id) on the source host.

        :param ctxt: The context used to run the method update_migrated_volume
        :param volume: The original volume that was migrated to this backend
        :param new_volume: The migration volume object that was created on
                           this backend as part of the migration process
        :param original_volume_status: The status of the original volume
        :returns: model_update to update DB with any needed changes
        """
        name_id = None
        provider_location = None
        root_helper = utils.get_root_helper()
        if original_volume_status == 'available':
            current_name = CONF.volume_name_template % new_volume['id']
            original_volume_name = CONF.volume_name_template % volume['id']
            
            current_zvol_name = self._zfs_volume({'name': current_name})
            original_zvol_name = self._zfs_volume({'name': original_volume_name})

            try:
                self._execute('zfs', 'rename',
                              current_zvol_name, original_zvol_name,
                              root_helper=root_helper,
                              run_as_root=True)
            except processutils.ProcessExecutionError:
                LOG.error(_('Unable to rename the logical volume '
                            'for volume: %s'), volume['id'])
                # If the rename fails, _name_id should be set to the new
                # volume id and provider_location should be set to the
                # one from the new volume as well.
                name_id = new_volume['_name_id'] or new_volume['id']
                provider_location = new_volume['provider_location']
        else:
            # The back-end will not be renamed.
            name_id = new_volume['_name_id'] or new_volume['id']
            provider_location = new_volume['provider_location']
        return {'_name_id': name_id, 'provider_location': provider_location}

    def create_volume_from_snapshot(self, volume, snapshot):
        """Creates a volume from a snapshot."""
        root_helper = utils.get_root_helper()

        # create temporary volume from snapshot
        # create snapshot of temporary volume
        # create volume from snapshot
        # promote temporary snapshot

        zvol_snapshot = self._zfs_snapshot(snapshot)
        zvol = self._zfs_volume(volume)

        try:
            self._execute('zfs', 'clone', zvol_snapshot, zvol,
                          root_helper=root_helper,
                          run_as_root=True)
        except processutils.ProcessExecutionError as exc:
            exception_message = (_("Failed to create volume from snapshot, "
                                    "error message was: %s")
                                    % six.text_type(exc.stderr))
            raise exception.VolumeBackendAPIException(
                data=exception_message)

        self.extend_volume(volume, volume['size'])

    def delete_volume(self, volume):
        """Deletes a logical volume."""

        if self._volume_not_present(volume):
            # If the volume isn't present, then don't attempt to delete
            return True

        root_helper = utils.get_root_helper()
        zvol = self._zfs_volume(volume)

        # Check zvol for snapshots
        try:
            out, err = self._execute('zfs', 'list', '-r', '-H',
                                     '-t', 'snapshot',
                                     zvol,
                                     root_helper=root_helper,
                                     run_as_root=True)
        except processutils.ProcessExecutionError:
            exception_message = (_("Failed to list snapshots "
                                    "error message was: %s")
                                    % six.text_type(exc.stderr))
            raise exception.VolumeBackendAPIException(
                data=exception_message)

        # At this point, clone-snap snapshots should be promoted
        snapshots = [x for x in out.splitlines() if "clone-snap" not in x]
        if len(snapshots) > 0:
            LOG.error(_('Unable to delete due to existing snapshot '
                        'for volume: %s'), volume['name'])
            raise exception.VolumeIsBusy(volume_name=volume['name'])

        # Delete zvol
        try:
            self._execute('zfs', 'destroy', '-r', zvol,
                          root_helper=root_helper,
                          run_as_root=True)
        except processutils.ProcessExecutionError as exc:
            exception_message = (_("Failed to delete zvol, "
                                    "error message was: %s")
                                    % six.text_type(exc.stderr))
            raise exception.VolumeBackendAPIException(
                data=exception_message)
        LOG.info(_('Successfully deleted volume: %s'), volume['id'])

    def create_snapshot(self, snapshot):
        """Creates a snapshot."""
        root_helper = utils.get_root_helper()
        zvol_snapshot = self._zfs_snapshot(snapshot)

        try:
            self._execute('zfs', 'snapshot', zvol_snapshot,
                          root_helper=root_helper,
                          run_as_root=True)
        except processutils.ProcessExecutionError as exc:
            exception_message = (_("Failed to create snapshot, "
                                    "error message was: %s")
                                    % six.text_type(exc.stderr))
            raise exception.VolumeBackendAPIException(
                data=exception_message)

    def delete_snapshot(self, snapshot):
        """Deletes a snapshot."""
        root_helper = utils.get_root_helper()
        zvol_snapshot = self._zfs_snapshot(snapshot)

        try:
            self._execute('zfs', 'list', '-H', zvol_snapshot,
                          root_helper=root_helper,
                          run_as_root=True)
        except processutils.ProcessExecutionError:
            # If the snapshot isn't present, then don't attempt to delete
            LOG.warning(_("snapshot: %s not found, "
                          "skipping delete operations"), snapshot['name'])
            LOG.info(_('Successfully deleted snapshot: %s'), snapshot['id'])
            return True
        
        try:
            out, err = self._execute('zfs', 'list', '-H',
                                     '-t', 'volume',
                                     '-o', 'name,origin',
                                     root_helper=root_helper,
                                     run_as_root=True)
        except processutils.ProcessExecutionError as exc:
            exception_message = (_("Failed to list origins, "
                                    "error message was: %s")
                                    % six.text_type(exc.stderr))
            raise exception.VolumeBackendAPIException(
                data=exception_message)

        for i in out.splitlines():
            name, origin = i.strip().split('\t')
            if origin == zvol_snapshot:
                raise exception.SnapshotIsBusy(snapshot_name=snapshot['name'])

        try:
            self._execute('zfs', 'destroy', zvol_snapshot,
                          root_helper=root_helper,
                          run_as_root=True)
        except processutils.ProcessExecutionError as exc:
            exception_message = (_("Failed to delete snapshot, "
                                    "error message was: %s")
                                    % six.text_type(exc.stderr))
            raise exception.VolumeBackendAPIException(
                data=exception_message)

    def local_path(self, volume, zpool=None):
        if zpool is None:
            zpool = self.configuration.zfs_zpool
        return "/dev/zvol/%s/%s" % (zpool, volume['name'])

    def copy_image_to_volume(self, context, volume, image_service, image_id):
        """Fetch the image from image_service and write it to the volume."""
        image_utils.fetch_to_raw(context,
                                 image_service,
                                 image_id,
                                 self.local_path(volume),
                                 self.configuration.volume_dd_blocksize,
                                 size=volume['size'])

    def copy_volume_to_image(self, context, volume, image_service, image_meta):
        """Copy the volume to the specified image."""
        image_utils.upload_volume(context,
                                  image_service,
                                  image_meta,
                                  self.local_path(volume))

    def create_cloned_volume(self, volume, src_vref):
        """Creates a clone of the specified volume."""
        LOG.info(_('Creating clone of volume: %s'), src_vref['id'])

        root_helper = utils.get_root_helper()
        try:
            self._execute('zfs-migrate',
                          self._zfs_volume(src_vref),
                          self._zfs_volume(volume),
                          root_helper=root_helper,
                          run_as_root=True)
        except Exception as e:
            with excutils.save_and_reraise_exception():
                LOG.error("Volume cloning failed due to "
                          "exception: %(reason)s.",
                          {'reason': six.text_type(e)}, resource=volume)

    def clone_image(self, context, volume,
                    image_location, image_meta,
                    image_service):
        return None, False

    def backup_volume(self, context, backup, backup_service):
        """Create a new backup from an existing volume."""
        volume = self.db.volume_get(context, backup.volume_id)
        snapshot = None
        if backup.snapshot_id:
            snapshot = objects.Snapshot.get_by_id(context, backup.snapshot_id)

        root_helper = utils.get_root_helper()
        temp_zvol = None
        temp_zvol_snapshot = None


        temp_snapshot = None
        # NOTE(xyang): If it is to backup from snapshot, back it up
        # directly. No need to clean it up.
        if snapshot:
            volume_path = self.local_path(snapshot)
        else:
            # NOTE(xyang): If it is not to backup from snapshot, check volume
            # status. If the volume status is 'in-use', create a temp snapshot
            # from the source volume, backup the temp snapshot, and then clean
            # up the temp snapshot; if the volume status is 'available', just
            # backup the volume.
            previous_status = volume.get('previous_status', None)
            if previous_status == "in-use":
                temp_snapshot = self._create_temp_snapshot(context, volume)
                backup.temp_snapshot_id = temp_snapshot.id
                backup.save()
                volume_path = self.local_path(temp_snapshot)
            else:
                volume_path = self.local_path(volume)

        try:
            with utils.temporary_chown(volume_path):
                with open(volume_path) as volume_file:
                    backup_service.backup(backup, volume_file)
        finally:
            # Destroy temporary volume (if exists)
            if temp_zvol:
                try:
                    self._execute('zfs', 'destroy', '-d', temp_zvol,
                                root_helper=root_helper,
                                run_as_root=True)
                except processutils.ProcessExecutionError as exc:
                    exception_message = (_("Failed to delete temporary zvol, "
                                            "error message was: %s")
                                            % six.text_type(exc.stderr))
                    raise exception.VolumeBackendAPIException(
                        data=exception_message)

            # Destroy temporary snapshot (if exists)
            if temp_zvol_snapshot:
                try:
                    self._execute('zfs', 'destroy', '-d', temp_zvol_snapshot,
                                root_helper=root_helper,
                                run_as_root=True)
                except processutils.ProcessExecutionError as exc:
                    exception_message = (_("Failed to delete temporary snapshot, "
                                            "error message was: %s")
                                            % six.text_type(exc.stderr))
                    raise exception.VolumeBackendAPIException(
                        data=exception_message)

    def restore_backup(self, context, backup, volume, backup_service):
        """Restore an existing backup to a new or existing volume."""
        volume_path = self.local_path(volume)
        with utils.temporary_chown(volume_path):
            with open(volume_path, 'wb') as volume_file:
                backup_service.restore(backup, volume['id'], volume_file)

    def get_volume_stats(self, refresh=False):
        """Get volume status.

        If 'refresh' is True, run update the stats first.
        """

        if refresh:
            self._update_volume_stats()

        return self._stats

    def extend_volume(self, volume, new_size):
        """Extend an existing volume's size."""
        LOG.info('extending volume to %s' % new_size)
        root_helper = utils.get_root_helper()

        zvol = self._zfs_volume(volume)

        try:
            self._execute('zfs', 'set',
                          'volsize=' + self._sizestr(new_size),
                          zvol,
                          root_helper=root_helper,
                          run_as_root=True)
        except processutils.ProcessExecutionError as exc:
            exception_message = (_("Failed to extend volume, "
                                    "error message was: %s")
                                    % six.text_type(exc.stderr))
            raise exception.VolumeBackendAPIException(
                data=exception_message)

    def manage_existing(self, volume, existing_ref):
        """Manages an existing ZVOL.

        Renames the ZVOL to match the expected name for the volume.
        Error checking done by manage_existing_get_size is not repeated.
        """
        root_helper = utils.get_root_helper()
        zvol_name = existing_ref['source-name']
        zvol_src = self._zfs_volume({'name': zvol_name})
        zvol_dst = self._zfs_volume(volume)

        try:
            self._execute('zfs', 'list', '-H', zvol_src,
                          root_helper=root_helper,
                          run_as_root=True)
        except processutils.ProcessExecutionError as exc:
            exception_message = (_("Failed to retrive zvol %(name)s, "
                                   "error message was: %(err_msg)s")
                                 % {'name': zvol_name,
                                    'err_msg': exc.stderr})
            raise exception.VolumeBackendAPIException(
                data=exception_message)

        vol_id = volutils.extract_id_from_volume_name(zvol_name)
        if volutils.check_already_managed_volume(vol_id):
            raise exception.ManageExistingAlreadyManaged(volume_ref=zvol_name)

        # Attempt to rename the ZVOL to match the OpenStack internal name.
        try:
            self._execute('zfs', 'rename', zvol_src, zvol_dst,
                          root_helper=root_helper,
                          run_as_root=True)
        except processutils.ProcessExecutionError as exc:
            exception_message = (_("Failed to rename logical volume %(name)s, "
                                   "error message was: %(err_msg)s")
                                 % {'name': zvol_name,
                                    'err_msg': exc.stderr})
            raise exception.VolumeBackendAPIException(
                data=exception_message)

    def manage_existing_object_get_size(self, existing_object, existing_ref,
                                        object_type):
        """Return size of an existing ZVOL for manage existing volume/snapshot.

        existing_ref is a dictionary of the form:
        {'source-name': <name of ZVOL>}
        """

        # Check that the reference is valid
        if 'source-name' not in existing_ref:
            reason = _('Reference must contain source-name element.')
            raise exception.ManageExistingInvalidReference(
                existing_ref=existing_ref, reason=reason)

        root_helper = utils.get_root_helper()
        zvol_name = existing_ref['source-name']
        zvol = self._zfs_volume({'name': zvol_name})

        try:
            out, err = self._execute('zfs', 'get', 'volsize', '-H', '-p',
                                     '-oname,value',
                                     zvol,
                                     root_helper=root_helper,
                                     run_as_root=True)
            name, size = out.strip().split('\t')
        except processutils.ProcessExecutionError:
            kwargs = {'existing_ref': zvol_name,
                      'reason': 'Specified logical volume does not exist.'}
            raise exception.ManageExistingInvalidReference(**kwargs)

        # Round up the size
        try:
            zvol_size = int(math.ceil(float(size) / units.Gi))
        except ValueError:
            exception_message = (_("Failed to manage existing %(type)s "
                                   "%(name)s, because reported size %(size)s "
                                   "was not a floating-point number.")
                                 % {'type': object_type,
                                    'name': zvol_name,
                                    'size': zvol_size})
            raise exception.VolumeBackendAPIException(
                data=exception_message)
        return zvol_size

    def manage_existing_get_size(self, volume, existing_ref):
        return self.manage_existing_object_get_size(volume, existing_ref,
                                                    "volume")

    def manage_existing_snapshot_get_size(self, snapshot, existing_ref):
        if not isinstance(existing_ref, dict):
            existing_ref = {"source-name": existing_ref}
        return self.manage_existing_object_get_size(snapshot, existing_ref,
                                                    "snapshot")

    def manage_existing_snapshot(self, snapshot, existing_ref):
        dest_name = snapshot['name']
        snapshot_temp = {"name": dest_name}
        if not isinstance(existing_ref, dict):
            existing_ref = {"source-name": existing_ref}
        return self.manage_existing(snapshot_temp, existing_ref)

    def _get_manageable_resource_info(self, cinder_resources, resource_type,
                                      marker, limit, offset, sort_keys,
                                      sort_dirs):
        entries = []
        cinder_ids = [resource['id'] for resource in cinder_resources]

        root_helper = utils.get_root_helper()
        try:
            out, err = self._execute('zfs', 'list', '-r', '-H', '-p',
                                     '-t', resource_type,
                                     '-oname,volsize',
                                     self.zpool,
                                     root_helper=root_helper,
                                     run_as_root=True)
        except processutils.ProcessExecutionError as exc:
            exception_message = (_("Failed to zfs list, "
                                    "error message was: %s")
                                    % six.text_type(exc.stderr))
            raise exception.VolumeBackendAPIException(
                data=exception_message)

        for entry in out.splitlines():
            name, size = entry.strip().split('\t')
            if resource_type == 'volume':
                potential_id = volutils.extract_id_from_volume_name(name)
            else:
                potential_id = volutils.extract_id_from_snapshot_name(name)

            info = {'reference': {'source-name': name},
                    'size': int(math.ceil(float(size) / units.Gi)),
                    'cinder_id': None,
                    'extra_info': None}

            if potential_id in cinder_ids:
                info['safe_to_manage'] = False
                info['reason_not_safe'] = 'already managed'
                info['cinder_id'] = potential_id
            else:
                info['safe_to_manage'] = True
                info['reason_not_safe'] = None

            if resource_type == 'snapshot':
                zpool, zvol, snapshot = name.replace('@', '/').split('/')
                info['source_reference'] = {'source-name': zvol}

            entries.append(info)

        return volutils.paginate_entries_list(entries, marker, limit, offset,
                                              sort_keys, sort_dirs)

    def get_manageable_volumes(self, cinder_volumes, marker, limit, offset,
                               sort_keys, sort_dirs):
        return self._get_manageable_resource_info(cinder_volumes, 'volume',
                                                  marker, limit,
                                                  offset, sort_keys, sort_dirs)

    def get_manageable_snapshots(self, cinder_snapshots, marker, limit, offset,
                                 sort_keys, sort_dirs):
        return self._get_manageable_resource_info(cinder_snapshots, 'snapshot',
                                                  marker, limit,
                                                  offset, sort_keys, sort_dirs)

    def retype(self, context, volume, new_type, diff, host):
        """Retypes a volume, allow QoS and extra_specs change."""

        LOG.debug('ZFS retype called for volume %s. No action '
                  'required for ZFS volumes.',
                  volume['id'])
        return True

    def migrate_volume(self, ctxt, volume, host, thin=False):
        """Optimize the migration if the destination is on the same server.
        If the specified host is another back-end on the same server, and
        the volume is not attached, we can do the migration locally without
        going through iSCSI.
        """
        false_ret = (False, None)
        if volume['status'] != 'available':
            return false_ret
        if 'location_info' not in host['capabilities']:
            return false_ret
        info = host['capabilities']['location_info']
        try:
            (dest_type, dest_hostname, dest_zpool, zfs_type) =\
                info.split(':')
        except ValueError:
            return false_ret
        if (dest_type != 'ZFSVolumeDriver' or dest_hostname != self.hostname):
            return false_ret

        if dest_zpool == self.zpool:
            message = (_("Refusing to migrate volume ID: %(id)s. Please "
                         "check your configuration because source and "
                         "destination are the same ZVOL: %(name)s.") %
                       {'id': volume['id'], 'name': self.zpool})
            LOG.error(message)
            raise exception.VolumeBackendAPIException(data=message)

        root_helper = utils.get_root_helper()
        try:
            self._execute('zfs', 'list', dest_zpool,
                          root_helper=root_helper, run_as_root=True)
        except processutils.ProcessExecutionError as exc:
            exception_message = (_("Destination ZFS Pool does not exist, "
                                   "error message was: %s")
                                   % six.text_type(exc.stderr))
            raise exception.VolumeBackendAPIException(
                data=exception_message)

        try:
            self._execute('zfs-migrate',
                          self._zfs_volume(volume, zpool=self.zpool),
                          self._zfs_volume(volume, zpool=dest_zpool),
                          root_helper=root_helper,
                          run_as_root=True)
        except Exception as e:
            with excutils.save_and_reraise_exception():
                LOG.error("Volume migration failed due to "
                          "exception: %(reason)s.",
                          {'reason': six.text_type(e)}, resource=volume)
        self.delete_volume(volume)
        return (True, None)
    
    def get_pool(self, volume):
        return self.backend_name

    def ensure_export(self, context, volume):
        volume_path = self.local_path(volume)
        model_update = \
            self.target_driver.ensure_export(context, volume, volume_path)
        return model_update

    def create_export(self, context, volume, connector, zpool=None):
        volume_path = self.local_path(volume, zpool)
        export_info = self.target_driver.create_export(
            context,
            volume,
            volume_path)
        return {'provider_location': export_info['location'],
                'provider_auth': export_info['auth'], }

    def remove_export(self, context, volume):
        self.target_driver.remove_export(context, volume)

    def initialize_connection(self, volume, connector):
        return self.target_driver.initialize_connection(volume, connector)

    def validate_connector(self, connector):
        return self.target_driver.validate_connector(connector)

    def terminate_connection(self, volume, connector, **kwargs):
        return self.target_driver.terminate_connection(volume, connector,
                                                       **kwargs)

