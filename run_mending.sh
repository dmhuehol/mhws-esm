#!/bin/bash

#SBATCH --partition=bar_all
#SBATCH --job-name=mending_ref
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=3
#SBATCH --output=logfile_mending_ref.txt

python mending.py