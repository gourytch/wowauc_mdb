#! /bin/bash
cd $(dirname $(readlink -f $0 ))
(
(
echo ""
echo ""
echo "============================="
stdbuf -oL -eL ./process_zipped.sh
echo ""
) &>var/process_zipped.log &
)