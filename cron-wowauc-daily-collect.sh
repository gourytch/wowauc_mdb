#! /bin/bash
BASE="$(dirname $(readlink -f $0))"
cd "$BASE"
. wowauc.conf

(
set -e
echo ""
echo "=== v v v === $(basename $0) started at $(date +'%F %T')"
./check_fstree.sh
./wowauc-satellites.sh
test -d input && rmdir input
mkdir -p input
cd $dir_fetched
rm -f *.bad
mv -t $BASE/input *.json
cd $BASE
python2.7 ./autonomous_processor.py
cd input
mv -t $BASE/$dir_fetched *.json
cd $BASE
rmdir input
./zip-fetched.sh
echo "=== ^ ^ ^ === $(basename $0) finished at $(date +'%F %T')"
echo ""
) >>$dir_log/$(basename $0 .sh).log 2>&1
