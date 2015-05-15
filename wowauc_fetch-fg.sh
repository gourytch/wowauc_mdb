#! /bin/bash

cd $(dirname $(readlink -f $0))

. wowauc.conf

./check_fstree.sh
(
date +"started at %Y-%d-%m %H:%M:%S"
python fetcher.py
date +"finished at %Y-%d-%m %H:%M:%S"
echo ""
) 2>&1 | tee -ai $dir_log/fetch.log
