#! /bin/bash
set -e
set -x

if [ "x$*" = "x" ]
then
  echo "parameter for datadir is not set"
  echo "use zip-fetched.sh or zip-imported.sh"
  exit 1
fi

datadir="$1"

cd $(dirname $0)
base=$(pwd)

source ./wowauc.conf

dataname="$(basename $datadir)"

abs_zipping=$(readlink -n -e $dir_zipping)
abs_zipped=$(readlink -n -e $dir_zipped)

zipdir="$abs_zipping/$dataname"



if [ -d $zipdir ]
then
  echo "directory $zipdir already exists. is there another backup process?"
  exit 1
fi

mkdir -p $abs_zipping
mv $datadir $zipdir
mkdir -p $datadir

cd $zipdir

ts_first=`ls -1 | sort -n | head -n1 | sed -e 's|\([0-9]\{8\}_[0-9]\{6\}\).*$|\1|'`
ts_last=`ls -1 | sort -n | tail -n1 | sed -e 's|\([0-9]\{8\}_[0-9]\{6\}\).*$|\1|'`

if [ "x$ts_first" = "x" ]; then
  echo "empty ts_first !"
  exit 1
fi

if [ "x$ts_last" = "x" ]; then
  echo "empty ts_last !"
  exit 1
fi


zname=$dataname-${ts_first}-${ts_last}.tar.xz

echo ""
echo "zip $name to $zname"

find . -print | grep -v '^\.$' | sort | \
tar --xz -cnvf $abs_zipping/$zname.tmp -T -

mv $abs_zipping/$zname.tmp $abs_zipped/$zname
cd $base
rm -rf "$zipdir"

