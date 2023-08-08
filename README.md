# cinder-zfs
ZoL (ZFS on Linux) driver for Cinder.

# ZFS on Linux installation
* [https://openzfs.github.io/openzfs-docs](https://openzfs.github.io/openzfs-docs)
* [https://ubuntu.com/tutorials/setup-zfs-storage-pool](https://ubuntu.com/tutorials/setup-zfs-storage-pool)

# Features
* Thin provisioning
* Migration between different zpools
* Snapshots
* Volume extending
* Python2/3 compatible

# Driver installation

```
$ git clone https://github.com/nusov/cinder-zfs
$ sudo cp cinder-zfs/bin/zfs-migrate /usr/local/bin
$ sudo chmod +x /usr/local/bin/zfs-migrate
$ sudo cp cinder-zfs/etc/cinder/rootwrap.d/zfs.filters /etc/cinder/rootwrap.d
$ sudo cp cinder-zfs/cinder/volume/drivers/zfs.py /usr/lib/python3/dist-packages/cinder/volume/drivers
```

# Pool creation
```
# zpool create cinder-zfs /dev/sdb
```

# Cinder configuration
```
[DEFAULT]
enabled_backends=zfs

[zfs]
volume_backend_name=zfs
volume_driver=cinder.volume.drivers.zfs.ZFSVolumeDriver
target_helper=lioadm
zfs_zpool=cinder-zfs
zfs_type=thin
```

# Usage
```
$ openstack volume create --size 1 cirros-boot --image cirros
+---------------------+--------------------------------------+
| Field               | Value                                |
+---------------------+--------------------------------------+
| attachments         | []                                   |
| availability_zone   | nova                                 |
| bootable            | false                                |
| consistencygroup_id | None                                 |
| created_at          | 2023-08-06T18:08:44.655261           |
| description         | None                                 |
| encrypted           | False                                |
| id                  | 4aa9bd98-dde4-40a5-b934-11aaa9aed056 |
| migration_status    | None                                 |
| multiattach         | False                                |
| name                | cirros-boot                          |
| properties          |                                      |
| replication_status  | None                                 |
| size                | 1                                    |
| snapshot_id         | None                                 |
| source_volid        | None                                 |
| status              | creating                             |
| type                | __DEFAULT__                          |
| updated_at          | None                                 |
| user_id             | a843b8e999b64c41a9a771672c473e33     |
+---------------------+--------------------------------------+

$ zfs list
NAME                                                     USED  AVAIL     REFER  MOUNTPOINT
cinder-zfs                                              44.9M  61.5G       96K  /cinder-zfs
cinder-zfs/volume-4aa9bd98-dde4-40a5-b934-11aaa9aed056  44.3M  61.5G     44.3M  -
```
