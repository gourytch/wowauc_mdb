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
echo ""
echo "=== v v v === $(basename $0) started at $(date +'%F %T')"
(
set -e
./check_fstree.sh
./wowauc-satellites.sh --clean
python2.7 ./autonomous_processor.py $BASE/$dir_fetched $BASE/processed
cd $dir_fetched
rm -f *.bad
cd $BASE
./zip-fetched.sh
)
rc="$?"
echo "=== ^ ^ ^ === $(basename $0) finished at $(date +'%F %T') with exitcode $rc"
echo ""

if [ $rc -ne 0 ]
then
  if which telegram_send.sh >/dev/null ; then
    telegram_send.sh error "$(basename $0) finished at $(date +'%F %T') with exitcode $rc"
  fi
fi

) >>$dir_log/$(basename $0 .sh).log 2>&1

rmdir $lockdir
