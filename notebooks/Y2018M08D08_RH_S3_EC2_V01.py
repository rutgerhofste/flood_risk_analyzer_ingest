
# coding: utf-8

# In[1]:

""" This notebook will download the data from S3 to the EC2 instance 
-------------------------------------------------------------------------------
In this notebook we will copy the data for the first couple of steps from WRI's
Amazon S3 Bucket. The data is large i.e. **40GB** so a good excuse to drink a 
coffee. The output in Jupyter per file is suppressed so you will only see a 
result after the file has been donwloaded. You can also run this command in your
terminal and see the process per file.

The script will rename and copy certain files to create a coherent dataset.

requires AWS cli to be configured.

Update 2019 06 26: rerun the script after Sam uploaded the new inundation layers.


Author: Rutger Hofste
Date: 20180808
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

Args:

    SCRIPT_NAME (string) : Script name
    OUTPUT_VERSION (integer) : output version for ec2 and s3.
    S3_INPUT_PATH (string) : input string for S3
    
Returns:

Result:
    Unzipped, renamed and restructured files in the EC2 output folder.


"""

# Input Parameters

SCRIPT_NAME = "Y2018M08D08_RH_S3_EC2_V01"
OUTPUT_VERSION = 2

#S3_INPUT_PATH = "s3://wri-projects/Aqueduct30/temp/inundationMaps/"
S3_INPUT_PATH = "s3://wri-projects/Aqueduct30/temp/inundationMaps/input_V02"

ec2_output_path = "/volumes/data/{}/output_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)

print("S3_INPUT_PATH: " + S3_INPUT_PATH + 
      "\nec2_output_path: " + ec2_output_path)



# In[2]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[3]:

get_ipython().system('rm -r {ec2_output_path}')
get_ipython().system('mkdir -p {ec2_output_path}')


# In[4]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH} {ec2_output_path} --recursive --exclude="*" --include="*.nc"')


# In[5]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous runs:  
# 0:03:19.741157
# 
# 

# In[ ]:



