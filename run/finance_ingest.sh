#!/bin/bash

if [ $# -ne 2 ]; then
        echo "please specify db-config-path and data-path"
		exit 1
fi

confpath=$1
mainpath=$2
ibespath="$mainpath/ibes/main/"
yahoopath="$mainpath/yahoo/history/"


python ./arango/ingest_csv.py --config-path ../conf/table/ibes.yaml --path "$ibespath" --backend-config-path "$confpath" --clean-start --batch-size=1500000
python ./arango/ingest_csv.py --config-path ../conf/table/ticker.yaml --path "$yahoopath" --backend-config-path "$confpath" --batch-size=1500000 --n-thread 5