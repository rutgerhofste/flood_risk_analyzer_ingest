
# coding: utf-8

# In[1]:

""" Ingest the inundation maps to earthegine
-------------------------------------------------------------------------------


Author: Rutger Hofste
Date: 20180816
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

Args:

    SCRIPT_NAME (string) : Script name
    OUTPUT_VERSION (integer) : output version for ec2 and s3.
    GCS_INPUT_PATH (string) : input string for GCS.
    EE_BASE_PATH (string) : Output path for earthengine, parent level.
    EE_OUTPUT_PATH (string) : output path for Earthengine.
    EC2_INPUT_PATH (string) : Ec2 input path for metadata. 
    
Returns:

Result:
    Images in one imageCollection on earthengine

"""
SCRIPT_NAME = "Y2018M08D16_RH_Floods_Inundation_EE_V01"
OUTPUT_VERSION = 6

GCS_INPUT_PATH = "gs://aqueduct30_v01/Y2018M08D16_RH_Convertt_Geotiff_V01/output_V05/output_V05" #typo in pathname
EE_BASE_PATH = "projects/WRI-Aquaduct/floods/{}".format(SCRIPT_NAME)
EE_OUTPUT_PATH = EE_BASE_PATH+"/output_V{:02.0f}".format(OUTPUT_VERSION)
EC2_INPUT_PATH = "/volumes/data/Y2018M08D16_RH_Convertt_Geotiff_V01/output_V05" #for metadata 

EE_IMAGECOLLECTION_NAME = "inundation"

print("GCS_INPUT_PATH:" + GCS_INPUT_PATH +
      "\nEC2_INPUT_PATH:" + EC2_INPUT_PATH +
      "\nEE_OUTPUT_PATH:" + EE_OUTPUT_PATH)



# In[2]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[3]:

import os
import pickle
import subprocess
import ast
import re
import sys
import ee
ee.Initialize()


# In[4]:

command = "earthengine create folder {}".format(EE_BASE_PATH)
response = subprocess.check_output(command,shell=True)
print(response)


# In[5]:

command = "earthengine create folder {}".format(EE_OUTPUT_PATH)
response = subprocess.check_output(command,shell=True)
print(response)


# In[6]:

EE_IC_PATH = "{}/{}".format(EE_OUTPUT_PATH,EE_IMAGECOLLECTION_NAME)

command = "earthengine create collection {}".format(EE_IC_PATH)
response = subprocess.check_output(command,shell=True)
print(response)


# In[7]:

def make_key_valid(key):
    #earthengine only allows letters, numbers and underscore. 
    # removing special characters, replacing hyphens with underscores. 
    new_key = re.sub('[^a-zA-Z0-9\-\_ \n\.]| ', '', key) #remove special characters
    new_key = re.sub('-',"_",new_key)
    
    #Property probabaly too long, replacing
    if len(key) > 40:
        new_key = new_key[0:39]
    
    return new_key


# In[8]:

def make_value_valid(value):
    # value must not exceed 1024 bytes 
    
    if sys.getsizeof(value) >= 1024:
        value = value[0:500] +  '  (too long, see netcdf for full details)..'

    return value


# In[9]:

def dict_to_command(d):
    # nodata value is specified separately since special tag required
    try:
        d.pop("inun__FillValue")
    except:
        pass    
    
    property_string = ""
    for key, value in d.items():        
        key = make_key_valid(key)
        value = make_value_valid(value)
        
        if key == "year" and value == 'hist':
            # running into errors (reserved keyword, renaming to avoid confusion)
            key = "year_string"

               
        property_string = property_string + " -p '{}'='{}' ".format(key,value) 

    return property_string


# In[10]:

def check_asset_exists(input_path):
    with open(input_path, 'rb') as handle:
        dictje = pickle.load(handle)
    filename = dictje["filename"]
    filename_no_ext, ext = filename.split(".")        
    asset_id = "{}/{}".format(EE_IC_PATH,filename_no_ext)
    try:
        ee.Image(asset_id).getInfo()
        exists = 1
    except:
        exists = 0
    return exists


# In[11]:

def create_ingest_command(input_path):
    with open(input_path, 'rb') as handle:
        dictje = pickle.load(handle)
        
    command = "earthengine upload image "    
    
    filename = dictje["filename"]
    filename_no_ext, ext = filename.split(".")
        
    asset_id = "{}/{}".format(EE_IC_PATH,filename_no_ext)
    command += "--asset_id={}".format(asset_id)    
    source_path = "{}/{}.tif".format(GCS_INPUT_PATH,filename_no_ext)    
    command += " {}".format(source_path)
    
    command += " --nodata_value=-9999 -p '(string)ingested_by=rutgerhofste' -p '(date)ingestion_date=2018-08-17'"
    
    # properties_from_filename    
    command +=  dict_to_command(dictje["properties_from_filename"])

    # global attributes
    command +=  dict_to_command(dictje["global_attributes"])
    
    #variable attributes
    command +=  dict_to_command(dictje["variable_attributes"])
    
    return command


# In[12]:

commands = {}
for root, dirs, files in os.walk(EC2_INPUT_PATH):
    for one_file in files:
        if one_file.endswith("pickle"):
            print(one_file)
            input_path = os.path.join(root,one_file)
            
            if check_asset_exists(input_path) == 1:
                print("file exists, skipping")
            elif check_asset_exists(input_path) == 0:
                print("file does not exist on earthengine yet, ingesting")
                ingest_command = create_ingest_command(input_path)
                commands[input_path] = {"ingest_command":ingest_command}
            
            
            
            
            
            


# In[ ]:

i = 1
for key, value in commands.items():
    print(i,key)
    ingest_command = value["ingest_command"]
    subprocess.check_output(ingest_command,shell=True)
    i+=1


# In[ ]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous runs:  
# 
