# nfvexpress-zfs
ZFS driver for Cinder. Currently works with ZoL (ZFS on Linux)

# pool create
zpool create cinder-zfs /dev/sdb 

# cinder configuration
```
[DEFAULT]
enabled_backends=zfs

[zfs]
volume_backend_name=zfs
volume_driver=cinder.volume.drivers.zfs.ZFSVolumeDriver
iscsi_helper=lioadm
zfs_zpool=cinder-zfs
zfs_type=thin
```

