#! /bin/bash

cd $(dirname $(readlink -f $0))
./wowauc_fetch-fg.sh &>/dev/null
