# cinder-zfs
ZFS driver for Cinder. Currently works with ZoL (ZFS on Linux).

# ZFS on Linux installation
* [https://openzfs.github.io/openzfs-docs](https://openzfs.github.io/openzfs-docs)
* [https://ubuntu.com/tutorials/setup-zfs-storage-pool](https://ubuntu.com/tutorials/setup-zfs-storage-pool)

# Features
* Thin provisioning
* Snapshots

# Driver installation

```
$ git clone https://github.com/nusov/cinder-zfs
$ sudo cp cinder-zfs/bin/zfs-migrate /usr/local/bin
$ sudo chmod +x /usr/local/bin/zfs-migrate
$ sudo cp cinder-zfs/etc/cinder/rootwrap.d/zfs.filters /etc/cinder/rootwrap.d
$ sudo cp cinder-zfs/cinder/volume/drivers/zfs.py /usr/lib/python3/dist-packages/cinder/volume/drivers
```

# Pool creation
zpool create cinder-zfs /dev/sdb 

# Cinder configuration
```
[DEFAULT]
enabled_backends=zfs

[zfs]
volume_backend_name=zfs
volume_driver=cinder.volume.drivers.zfs.ZFSVolumeDriver
iscsi_helper=tgtadm
zfs_zpool=cinder-zfs
zfs_type=thin
```

