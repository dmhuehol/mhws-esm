#!/bin/bash

#SBATCH --partition=bar_all
#SBATCH --job-name=shatter_ref
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=3
#SBATCH --output=logfile_shatter_ref.txt

python shatter.py