#!/bin/bash

#SBATCH --partition=hur_all
#SBATCH --job-name=calc_mhws_ssp245
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=3
#SBATCH --output=logfile_calc_mhws_ssp245.txt

### Load modules
module load conda/latest
conda activate dh-env

python calc_shard_mhws.py