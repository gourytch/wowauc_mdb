#! /bin/bash
cd $(dirname $(readlink -f $0))
sudo docker build --rm=true --tag="gour/wowauc_leech" ./image_build
