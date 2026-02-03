#!/usr/bin/env bash
# This script modifies system-level configurations required for docker host

# Remove process number limit and configure filesystem mount options
# echo "LABEL=cloudimg-rootfs   /        ext4   defaults,noatime,nodiratime,barrier=0       0 0" > fstab
# sudo cp fstab /etc/fstab

# Configure ulimit settings for current user
echo "ulimit -n 65535" >> ~/.profile
# Cannot assign a value more than half of `/proc/sys/kernel/threads-max`, which is about 120,000.
echo "ulimit -u 60000" >> ~/.profile

# Configure system-wide resource limits
echo "*            -          nproc     65535 " | sudo tee -a /etc/security/limits.conf
echo "*            -          nfile     65535 " | sudo tee -a /etc/security/limits.conf

# Configure systemd task limits
echo "DefaultTasksMax=65535" | sudo tee -a /etc/systemd/system.conf
sudo mkdir -p /etc/systemd/logind.conf.d
echo "[Login] \nUserTasksMax=infinity" | sudo tee -a /etc/systemd/logind.conf.d/override.conf

sudo apt update
sudo apt install -y iotop