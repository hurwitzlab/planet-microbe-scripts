#!/bin/bash

DELETEALL="-x"

for path in `ls -d ../../planet-microbe-datapackages/*`
do
    if test -f "$path/datapackage.json"; then
        ./load_datapackage_postgres2.py $DELETEALL -u mbomhoff -d pm_test $path/datapackage.json
    fi
    DELETEALL=""
done
