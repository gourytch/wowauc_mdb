#! /bin/bash
set -e
set -x

cd $(dirname $(readlink -f $0))
. wowauc.conf
. wowauc-satellites.conf
./check_fstree.sh

lockdir="$locker/$(basename $0)"
if ! mkdir $lockdir ; then
  echo "Lock failed - exit" >&2
  exit 1
fi

do_clean=false
case "$1" in
--clean)
  do_clean=true
  ;;
*)
  ;;
esac


mkdir -p $DESTINATION
echo "sources   : $SSH_SATELLITES"
echo "collector : $DESTINATION"

log="$(basename $0) processing log:"

for satellite in $SSH_SATELLITES
do
(
set -e

if $do_clean
then
############

cat << __EOF_CAT__

===
=== process satellite $satellite ===
===

__EOF_CAT__


ssh $satellite /bin/bash -e -x << __EOF_PREPARE__
cd $BASE
test -d $DAILY_DIRNAME
test ! -d $RSYNC_DIRNAME || rmdir $RSYNC_DIRNAME
mkdir -p $RSYNC_DIRNAME
cd $DAILY_DIRNAME
rm -f *.bad || true
mv -t $BASE/$RSYNC_DIRNAME *.json || true
__EOF_PREPARE__

src="$satellite:$BASE/$RSYNC_DIRNAME/"
rsync -cruvz "$src" $DESTINATION 2>&1 | tee -ai "${LOGFILE}"
ssh $satellite /bin/bash -e -x << __EOF_CLEANUP__
cd $BASE/$RSYNC_DIRNAME
rm -f *.json || true
cd $BASE
rmdir $RSYNC_DIRNAME
__EOF_CLEANUP__

#################
else # ! do_clean
#################

src="$satellite:$BASE/$DAILY_DIRNAME/"
rsync -cruvz "$src" --exclude '*.bad' --exclude '*.tmp' $DESTINATION 2>&1 | tee -ai "${LOGFILE}"

#################
fi
)
rc="$?"

if [ $rc -ne 0 ]
then
    log="$log
<$satellite> failed"
else
    log="$log
<$satellite> processed"
fi
 
done

if which telegram_send.sh >/dev/null ; then
  telegram_send.sh "$log"
fi

rmdir $lockdir
