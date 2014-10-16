#! /bin/bash
set -e
set -x

cd $(dirname $0)
source ./wowauc.conf
exec ./zip-datadir.sh "$dir_fetched"

