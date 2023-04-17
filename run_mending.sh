#!/bin/bash -l
### Job Name
#PBS -N mend_shards_mhws
### Project code
#PBS -A P06010014
#PBS -l walltime=23:59:00
#PBS -q casper
### Merge output and error files
#PBS -j oe
### Select 1 nodes with 3 CPUs each
#PBS -l select=1:ncpus=30:mem=300GB
### Send email on abort, begin and end
#PBS -m abe
### Specify mail recipient
#PBS -M dhueholt@rams.colostate.edu
exec &> logfile_calc_mhws.txt

export TMPDIR=/glade/scratch/dhueholt/temp
mkdir -p $TMPDIR

### Load modules
module load conda/latest
conda activate dh-env

python mending.py