#!/bin/bash
set -e

USERNAME="$1"
DBNAME="$2"
PASSWORD=$3
if test -n "$PASSWORD"; then PASSWORD="-p $PASSWORD"; fi
IRODSPATH=$4
if test -n "$IRODSPATH"; then IRODSPATH="-i $IRODSPATH"; fi
DELETEALL="-x"
DATAPACKAGES="\
../../planet-microbe-datapackages/Amazon_continuum_plume \
../../planet-microbe-datapackages/Amazon_continuum_plume_metatranscriptomes \
../../planet-microbe-datapackages/Amazon_continuum_metatranscriptomes_polyA \
../../planet-microbe-datapackages/Amazon_continuum_river \
../../planet-microbe-datapackages/Amazon_continuum_river_metatranscriptomes \
../../planet-microbe-datapackages/BATS_Chisholm \
../../planet-microbe-datapackages/CDEBI_mid_range \
../../planet-microbe-datapackages/GOS_2009-10 \
../../planet-microbe-datapackages/HOT_DeLong_Timedepth_series \
../../planet-microbe-datapackages/HOT-Chisholm \
../../planet-microbe-datapackages/HOT-DeLong \
../../planet-microbe-datapackages/HOT_Delong_metatranscriptomes \
../../planet-microbe-datapackages/OSD \
../../planet-microbe-datapackages/Tara_Oceans \
../../planet-microbe-datapackages/Tara_Oceans_Polar \
"

#for path in `ls -d ../../planet-microbe-datapackages/*`
for path in $DATAPACKAGES
do
    file=$path/datapackage.json
    if test -f $file; then
        ./load_datapackage_postgres.py --nowarn $DELETEALL -u $USERNAME -d $DBNAME $PASSWORD $IRODSPATH $file
        DELETEALL=""
    fi
done
