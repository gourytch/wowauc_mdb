#! /bin/bash
cd $(dirname $(readlink -f $0))
img="gour/wowauc_leech"
sudo docker run --rm=true -v $(pwd)/data:/data $img
