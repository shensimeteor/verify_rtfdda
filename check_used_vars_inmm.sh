#!/bin/bash

for var in $(cat vars.flexinput.mm); do
    echo $var "================================"
    grep $var veri_rtfdda_MC_WRF.1_.pl
done
