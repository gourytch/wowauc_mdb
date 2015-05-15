#! /bin/bash
set -e
set -x

cd $(dirname $(readlink -f $0))
source ./wowauc.conf
./check_fstree.sh
exec ./zip-datadir.sh "$dir_fetched"

