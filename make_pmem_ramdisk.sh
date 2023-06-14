#/bin/bash
mkdir -p /tmp/ramdisk
sudo mount -t tmpfs -o size=2G myramdisk /tmp/ramdisk
mkdir -p /tmp/ramdisk/data
mount | tail -n 1