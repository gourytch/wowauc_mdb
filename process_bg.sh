#! /bin/bash
cd $(dirname $(readlink -f $0 ))
(
(
echo "============================="
echo "STARTED AT $(date +'%F %T')"
stdbuf -oL -eL ./process_zipped.sh
echo "RETURNED: $?"
echo "FINISHED AT $(date +'%F %T')"
echo ""
echo ""
echo ""
) &>var/process_zipped.log &
)