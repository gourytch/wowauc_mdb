#! /bin/bash
set -e
set -x
cd $(dirname $(readlink -f $0))

zipped=zipped
zipped_processed=zipped_processed
unzipped=input

test -d $zipped
test -d $zipped_processed || mkdir -p $zipped_processed
test -d $unzipped || mkdir -p $unzipped
rm -f $unzipped/*.json $unzipped/*.json.bad

for z in $zipped/*.xz
do
  echo "process $z"
  tar xapf $z -C $unzipped
  python2.7 autonomous_processor.py
  mv -t $zipped_processed $z 
  rm -f $unzipped/*.json $unzipped/*.json.bad
done

