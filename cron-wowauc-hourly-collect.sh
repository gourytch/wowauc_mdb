#! /bin/bash
BASE="$(dirname $(readlink -f $0))"
cd "$BASE"
. wowauc.conf
./check_fstree.sh
lockdir="$locker/$(basename $0)"
if ! mkdir $lockdir ; then
  echo "Lock failed - exit" >&2
  exit 1
fi

(
set -e
echo ""
echo "=== v v v === $(basename $0) started at $(date +'%F %T')"
./check_fstree.sh
./wowauc-satellites.sh
python2.7 ./autonomous_processor.py $BASE/$dir_fetched $BASE/processed
echo "=== ^ ^ ^ === $(basename $0) finished at $(date +'%F %T')"
echo ""
) >>$dir_log/$(basename $0 .sh).log 2>&1

rmdir $lockdir
