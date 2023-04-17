#!/bin/bash -l
### Job Name
#PBS -N selyear_20112014
### Project code
#PBS -A P06010014
#PBS -l walltime=30:00
#PBS -q casper
### Merge output and error files
#PBS -j oe
### Select 1 nodes with 1 CPUs each
#PBS -l select=1:ncpus=1:mem=10GB
### Send email on abort, begin and end
#PBS -m abe
### Specify mail recipient
#PBS -M dhueholt@rams.colostate.edu
# exec &> logfile_ssp245_copy.txt

# export TMPDIR=/glade/scratch/dhueholt/temp
# mkdir -p $TMPDIR
module load cdo

IN_PATH="/glade/scratch/dhueholt/daily_SST/"
# IN_PATH="/Users/dhueholt/Documents/mhws_data/daily_SST/full/"
IN_TOKEN="*.SST.*"
STRT_YR=2015
END_YR=2024
OUT_PATH="/glade/scratch/dhueholt/daily_SST/defPeriod/"
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
