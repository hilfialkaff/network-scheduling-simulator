#!/bin/bash

sudo apt-get update
sudo apt-get install -y openjdk-6-jre openjdk-6-jdk

sudo chown halkaff /mnt/extra -R
tar -xzpvf hadoop-1.1.2.tar.gz -C /mnt/extra
cp conf /mnt/extra/hadoop-1.1.2/ -r
sudo chown halkaff /mnt/extra -R
sudo cp hosts /etc/hosts

cd ../dotfiles/
./setup.sh
