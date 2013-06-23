#!/bin/bash

sudo apt-get update
sudo apt-get install -y openjdk-6-jre openjdk-6-jdk

cp conf .. -r
sudo cp hosts /etc/hosts

cd ~/dotfiles/
./setup.sh
