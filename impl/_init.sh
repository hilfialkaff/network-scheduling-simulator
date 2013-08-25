#!/bin/bash

echo -e "10.0.0.2\tmaster" | sudo tee -a /etc/hosts
sudo apt-get update
sudo apt-get install -y openjdk-6-jre openjdk-6-jdk maven build-essential protobuf-compiler autoconf automake libtool cmake zlib1g-dev pkg-config libssl-dev

sudo ./init.sh
