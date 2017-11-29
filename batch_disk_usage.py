#!/usr/bin/env python2.7
#-----------------------------------------------------------------------
# Program: batch_disk_usage.py 
# Purpose: To display disk usage for batch.
# Created: Sarah Sparrow 20/02/17
#-----------------------------------------------------------------------
import os,glob
import subprocess
import csv
import argparse

def du(path):
    return subprocess.check_output(['du','-s', path]).split()[0].decode('utf-8')

def write_csv(outfile,batch_size_dict):
    with open(outfile, 'wb') as csvfile:
    	batch_sizes = csv.writer(csvfile,delimiter=',')
   	batch_sizes.writerow(["Batch", "Size (Tb)"])
 	for batch,size in batch_size_dict.iteritems():
    		batch_sizes.writerow([batch,size])

def get_dirs(ulserver):
    if ulserver=="upload2":
	out_dir1='/gpfs/projects/cpdn/storage/boinc/upload/'
	out_dir2='/gpfs/projects/cpdn/storage/boinc/project_results/'
      	
	ddirs1=glob.glob(out_dir1+'batch*')
    	ddirs2=[fn for fn in glob.glob(out_dir1+'*/batch*') 
         	if not os.path.basename(fn).endswith('.gz')]
    	ddirs3=glob.glob(out_dir2+'*/batch*')

    	ddirs=ddirs1+ddirs2+ddirs3 

    if ulserver=="upload3":
	out_dir1='/group_workspaces/jasmin2/cpdn_rapidwatch/results/'
	out_dir2='/group_workspaces/jasmin/cssp_china/wp1/lotus/cpdn/'
	out_dir3='/group_workspaces/jasmin2/cpdn_rapidwatch/disks/rapid-watch1/storage/boinc/upload'
        out_dir4='/group_workspaces/jasmin2/gotham/gotham/cpdn_data/'	
	ddirs1=glob.glob(out_dir1+'batch*')
	ddirs2=[fn for fn in glob.glob(out_dir1+'*/batch*')
                if not os.path.basename(fn).endswith('.gz')]
	ddirs3=glob.glob(out_dir2+'*/batch*')
	ddirs4=glob.glob(out_dir3+'*/batch*')
	ddirs5=glob.glob(out_dir4+'*/batch*')

	ddirs=ddirs1+ddirs2+ddirs3+ddirs4+ddirs5
    return ddirs
	
def main():
    # First read in the upload server specified
    parser = argparse.ArgumentParser(description="Find batch data size for upload server")
    parser.add_argument("ulserver", type=str, help="the upload server")
    args = parser.parse_args()    

    ddirs=get_dirs(args.ulserver)
	    
    batch_size_dict={}

    for ddir in ddirs:
	try:
		batchdir=ddir.split("/")[-1]
		batchno=batchdir.split("batch")[-1].split("_")[-1]
    		#Default output is in kilobytes - convert to terabytes	
		batch_size=float(du(ddir))/1.e9
		print batchno, batch_size
		batch_size_dict[batchno]=batch_size

    		write_csv(args.ulserver+"_storage.csv",batch_size_dict)
	except:
		print "Cannot read folder ",ddir

if __name__ == "__main__":
    main()
