#!/bin/bash

#SBATCH --partition=bar_all
#SBATCH --job-name=selyear
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=3
#SBATCH --output=logfile_selyear.txt

IN_PATH="scratch/dhueholt/daily_SST/"
# IN_PATH="/Users/dhueholt/Documents/mhws_data/daily_SST/full/"
IN_TOKEN="*.SST.*"
STRT_YR=2015
END_YR=2024
OUT_PATH="/scratch/dhueholt/daily_SST/defPeriod/"
# OUT_PATH="/Users/dhueholt/Documents/mhws_data/daily_SST/"

IN_CARD="$IN_PATH$IN_TOKEN"
PATH_LENGTH=${#IN_PATH}
for f in $IN_CARD; do
    ACTIVE_FNAME=${f:$PATH_LENGTH}
    ACTIVE_TIME=$(echo $ACTIVE_FNAME | cut -d'.' -f12)
    STRT_MD="0101"
    END_MD="1231"
    RG="_RG"
    OUT_FNAME=${ACTIVE_FNAME//$ACTIVE_TIME/$STRT_YR$STRT_MD"-"$END_YR$END_MD$RG}
    OUT_CARD=$OUT_PATH$OUT_FNAME
    echo $OUT_CARD
    cdo selyear,$STRT_YR/$END_YR $f $OUT_CARD
done
