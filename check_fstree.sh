#! /bin/bash
cd "$(dirname $(readlink -f $0))"
. wowauc.conf
mkdir -p \
  $dir_etc $dir_log $locker \
  $dir_fetching $dir_fetched \
  $dir_importing $dir_imported \
  $dir_zipping $dir_zipped \
  $dir_fetched_items $dir_importing_items $dir_imported_items \
  $dir_zipping_items $dir_zipped_items