#! /bin/bash

cd "$(dirname $(readlink -f $0))"
test -d ./var/uz && rm -rf ./var/uz

function timer_start() {
  if [ "x$TS_START" = "x" ] ; then
    export TS_START=$(date +'%s')
  fi
}


function timer_finish() {
  if [ "x$TS_START" = "x" ]; then
    echo "timer_start must be invoked before!" 1>&2
    return
  fi
  export TS_FINISH=$(date +'%s')
  local s=$[$TS_FINISH - $TS_START]
  local d=$[$s / 86400] ; s=$[$s - $d * 86400]
  local h=$[$s / 3600] ; s=$[$s - $h * 3600]
  local m=$[$s / 60] ; s=$[$s - $m * 60]
  echo "STARTED AT  : $(date +'%F %T' --date @$TS_START)"
  echo "FINISHED AT : $(date +'%F %T' --date @$TS_FINISH)"
  if [ $d -gt 0 ]; then
    printf "DURATION IS : %d days %02d:%02d:%02d\n" $d $h $m $s
  else
    printf "DURATION IS : %02d:%02d:%02d\n" $h $m $s
  fi
  unset TS_START
  unset TS_FINISH
}


###########################################
timer_start
(
set -e
for z in ./var/xz/*.tar.xz ; do
  zz="$(readlink -f $z)"
  echo "=== process $zz"
  mkdir -p ./var/uz
  cd ./var/uz
  tar xapf $zz
  cd ../..
  if [ -f ./var/first ]; then
    add="--new"
    rm ./var/first
  else
    add=""
  fi
  python ./import_auc.py $add --debug ./var/uz/
  rm -rf ./var/uz
  mv $zz ./var/xz-bak/
done
)
timer_finish
