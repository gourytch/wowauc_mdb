#! /bin/bash

cd $(dirname $(readlink -f $0))
. wowauc.conf

[ -d $dir_importing ] || mkdir -p $dir_importing

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
) >>var/log/wowitem_fetch.log 2>&1
rm -f $lock
