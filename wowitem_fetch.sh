#! /bin/bash

cd $(dirname $(readlink -f $0))
. wowauc.conf

./check_fstree.sh

lock=$dir_importing/wowitem_fetch.lock
if [ -e $lock ]; then
  exit 0;
fi

date +'%F %T' >$lock
(
  echo "STARTED AT: $(date +'%F %T')"
  python ./wowitem_fetch.py
  echo "FINISHED AT: $(date +'%F %T')"
  echo ""
  echo ""
) >>$dir_log/wowitem_fetch.log 2>&1
rm -f $lock
