#!/bin/bash
#archive upper level wrfout for verification
narg=$#
if [ $narg -ne 1 ]; then
    echo "<usage> $0 <save_dir>"
    exit 2
fi
savedir=$1
timex="00 12"
domx="2"
for tx in $timex; do
    for domx in $domx; do
        for f in $(ls wrfout_d0${domx}_????-??-??_${tx}:00:00*); do
            echo "to archive $f .. "
            ncks -v "T,U,V,QVAPOR,P,PB,XLAT,XLONG,PH,PHB,HGT,ZNU,ZNW,Times" $f -o $savedir/$f
        done
    done
done

