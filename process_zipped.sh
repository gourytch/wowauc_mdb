#! /bin/bash
set -e
set -x

cd "$(dirname $(readlink -f $0))"
test -d ./var/uz && rm -rf ./var/uz

first=1

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
