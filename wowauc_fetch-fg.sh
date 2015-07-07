#! /bin/bash

cd $(dirname $(readlink -f $0))

. wowauc.conf
./check_fstree.sh
lockdir="$locker/$(basename $0)"
if ! mkdir $lockdir ; then
  echo "Lock failed - exit" >&2
  exit 1
fi

(
date +"started at %Y-%d-%m %H:%M:%S"
python fetcher.py
date +"finished at %Y-%d-%m %H:%M:%S"
echo ""
) 2>&1 | tee -ai $dir_log/fetch.log

rmdir $lockdir
