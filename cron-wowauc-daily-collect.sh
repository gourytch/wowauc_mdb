#! /bin/bash
BASE="$(dirname $(readlink -f $0))"
cd "$BASE"
. wowauc.conf

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
echo "=== ^ ^ ^ === $(basename $0) finished at $(date +'%F %T') with exitcode $?"
echo ""
) >>$dir_log/$(basename $0 .sh).log 2>&1
