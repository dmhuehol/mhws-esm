''' calc_shard_mhws 
Calculate MHWs from shard files in parallel using multiprocessing. Run
"shatter" first to break data up into manageable shard files.

#### INPUTS ####
dataDict: defines input data
    dataPath: input path of data shards
    dataToken: suggested tokens for datasets below
        ARISE-SAI-1.5: '*SSP245-TSMLT-GAUSS*'
        SSP2-4.5: '*BWSSP245*'
    refPath: input path for reference shards
    refToken: token for reference shards--see dataToken for options
    
NOTE: The multiprocessing documentation claims 'fork' functionality only works 
on Unix-based systems (e.g., Mac OS, Linux). Thus, this code likely will not
run on Windows, although I have not verified this.    

Written by Daniel Hueholt
Graduate Research Assistant at Colorado State University
'''
import glob
import multiprocessing as mp
import sys

from icecream import ic
import fun_mhws as fm

#### INPUTS ####
dataDict = { 
    "dataPath": '/Users/dhueholt/Documents/mhws_data/shards/data/', # Path of data shards
    "dataToken": '*BWSSP245*', # See docstring for tokens
    "refPath": '/Users/dhueholt/Documents/mhws_data/shards/ref/', # Path of reference shards
    "refToken": '*BWSSP245*' # See docstring for tokens
}
outDict = {
    "outPath": '/Users/dhueholt/Documents/mhwwg_out/', # Path to save MHW files
    "outPrefix": '', # Prefix to append to output filenames
    "saveFlag": True # True/False to save/not save output files
}
numProc = 7 # Number of files to parallel process; set to threads-1 (e.g., multiprocessing.cpu_count()-1) for best efficiency

# Match up data shards and reference shards
dataGlobs = sorted(glob.glob(dataDict["dataPath"] + dataDict["dataToken"]))
refGlobs = sorted(glob.glob(dataDict["refPath"] + dataDict["refToken"]))
for rg in refGlobs:
    ic(rg) # Track progress
    refFrg = rg.split('_')[-1]
    for dc,dg in enumerate(dataGlobs):
        if refFrg in dg:
            if __name__ == '__main__':
                mp.set_start_method('fork', force=True)
                proc = mp.Process( # Each parallel process calculates MHWs for a data shard
                    target=fm.calc_marine_heatwaves, args=(dg, rg, outDict))
            if dc % numProc == 0 and dc !=0:
                proc.start()
                proc.join() # Run numProc+1 processes to completion before starting more
                proc.close() # Free up all resources from previous proc
            else:
                proc.start()