
# coding: utf-8

# In[1]:

"""
Convert netCDF files to geotiff.

Update 2019 06 26: rerun using updated inundation layers

Author: Rutger Hofste
Date: 20180816
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04


Args:
    TESTING (boolean) : Toggle testing mode
    SCRIPT_NAME (string) : Script name
    OUTPUT_VERSION (integer) : output version for ec2 and s3.
    GCS_OUTPUT_PATH (string) : Output path for Google Cloud Storage.
    
Returns:

Result:
    Geotiff and pickled dictionaries in ec2_output folder.

"""
TESTING = 0

SCRIPT_NAME = "Y2018M08D16_RH_Convertt_Geotiff_V01"
OUTPUT_VERSION = 5

EC2_INPUT_PATH = "/volumes/data/Y2018M08D08_RH_S3_EC2_V01/output_V02/"
GCS_OUTPUT_PATH = "gs://aqueduct30_v01/{}/output_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)

ec2_output_path = "/volumes/data/{}/output_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)
s3_output_path = "s3://wri-projects/Aqueduct30/processData/{}/output_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)


print("EC2_INPUT_PATH: " + EC2_INPUT_PATH + 
      "\nec2_output_path: " + ec2_output_path,
      "\ns3_output_path: " + s3_output_path,
      "\nGCS_OUTPUT_PATH:" + GCS_OUTPUT_PATH)


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

import os
import pandas as pd
import netCDF4
import pickle
import multiprocessing as mp
import numpy as np


try:
    from osgeo import ogr, osr, gdal
except:
    sys.exit('ERROR: cannot find GDAL/OGR modules')

if 'GDAL_DATA' not in os.environ:
    os.environ['GDAL_DATA'] = r'/usr/share/gdal/2.1'


# In[5]:

def filename_to_dict(filename):
    values = filename.split("_")
    
    value_length = len(values)
    
    if value_length == 5:
        keys = ["floodtype","climate","model","year","returnperiod"]
        dictje = dict(zip(keys,values))
    elif value_length == 6:
        keys = ["floodtype","climate","subsidence","year","returnperiod","returnperiod_decimal"]
        dictje = dict(zip(keys,values))
    elif value_length == 8:
        keys = ["floodtype","climate","subsidence","year","returnperiod","returnperiod_decimal","model","sea_level_rise_scenario"]
        dictje = dict(zip(keys,values))
    else:
        print("error")        
    return dictje

def ncdump(nc_fid):
    '''ncdump outputs dimensions, variables and their attribute information.
    -------------------------------------------------------------------------------
    
    The information is similar to that of NCAR's ncdump utility.
    ncdump requires a valid instance of Dataset.

    Args:
        nc_fid (netCDF4.Dataset) : A netCDF4 dateset object

    Returns:
        nc_attrs (list) : A Python list of the NetCDF file global attributes.
        nc_dims (list) : A Python list of the NetCDF file dimensions.
        nc_vars (list) : A Python list of the NetCDF file variables.
    '''

    nc_attrs = nc_fid.ncattrs()
    nc_dims = [dim for dim in nc_fid.dimensions]  # list of nc dimensions
    nc_vars = [var for var in nc_fid.variables]  # list of nc variables
    return nc_attrs, nc_dims, nc_vars

def get_global_attributes(dictje):
    """ Get global attributes from netcdf
    
    Args:
        dictionary with root, filename and properties.
    
    """
    
    input_path = os.path.join(dictje["root"],dictje["filename"])
    nc_fid = netCDF4.Dataset(input_path, 'r')
    nc_attrs, nc_dims, nc_vars = ncdump(nc_fid)
    
    global_attributes_dict = {}
    for nc_attr in nc_attrs:
        global_attributes_dict[nc_attr] = nc_fid.getncattr(nc_attr)
        
    
    return global_attributes_dict
 
def get_variable_attributes(dictje):
    """ Get global attributes from netcdf
    
    Args:
        dictionary with root, filename and properties.
    
    """
    
    input_path = os.path.join(dictje["root"],dictje["filename"])
    nc_fid = netCDF4.Dataset(input_path, 'r')
    nc_attrs, nc_dims, nc_vars = ncdump(nc_fid)
    parameter = nc_vars[-1] #warning, project dependent
    
    variable_attrs = nc_fid.variables[parameter].ncattrs()
    
    variable_attributes_dict = {}
    for variable_attr in variable_attrs:
            variable_attributes_dict[parameter+"_"+variable_attr] = nc_fid.variables[parameter].getncattr(variable_attr)
    
    
    return variable_attributes_dict

def write_geotiff(output_path,geotransform,geoprojection,data,nodata_value=-9999,datatype=gdal.GDT_Float32):
    
    """ Write data to geotiff file
    -------------------------------------------------------------------------------
    
    Args: 
        output_path (string) : output_path 
        geotransform (tuple) : geotransform
        geoprojection (string) : geoprojection in osr format
        data (np.array) : numpy array    
        nodata_value (integer) : NoData value
        datatype (GDAL datatype)
    
    """  
    
    (x,y) = data.shape
    format = "GTiff"
    driver = gdal.GetDriverByName(format)
    # you can change the dataformat but be sure to be able to store negative values including -9999
    dst_ds = driver.Create(output_path,y,x,1,datatype, [ 'COMPRESS=LZW' ])
    dst_ds.GetRasterBand(1).SetNoDataValue(nodata_value)
    dst_ds.GetRasterBand(1).WriteArray(data)
    dst_ds.SetGeoTransform(geotransform)
    dst_ds.SetProjection(geoprojection)
    dst_ds = None
    return 1

def get_global_georeference(array):
    """ Get the geotransform and projection for a numpy array
    -------------------------------------------------------------------------------
    
    Returns a geotransform and projection for a global extent in epsg 4326 
    projection.
    
    Args:
        array (np.array) : numpy array
    
    Returns:
        geotransform (tuple) : geotransform
        geoprojection (string) : geoprojection in osr format    
    
    """
    
    y_dimension = array.shape[0] #rows, lat
    x_dimension = array.shape[1] #cols, lon
    geotransform = (-180,360.0/x_dimension,0,90,0,-180.0/y_dimension)
    
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(4326)
    geoprojection = srs.ExportToWkt()
    
    if len(geoprojection) == 0:
        warnings.warn("GDAL_DATA path not set correctly. Assert os.environ "                       "contains GDAL_DATA \n"                       "Code will execute without projection set")

    return geotransform, geoprojection

def standardize_time(time_unit,times):
    """ Append standardize time to list
    -------------------------------------------------------------------------------
    
    The netCDF results of the university of Utrecht consist of multiple time 
    formats. 
    
    Args:
        time_unit (string) : units as provided by the netCDF4 file. 
        times (list) : list of time in units provided in time_units (e.g. days).
    
    Returns:
        standardized_time (list) : list of normalized times in datetime format.
    
    """
    

    
    standardized_time =[]
    for time in times:
        if time_unit == ("days since 1900-01-01 00:00:00") or (time_unit =="Days since 1900-01-01"):
            standardized_time.append(datetime.datetime(1900,1,1) + datetime.timedelta(days=time))
        elif time_unit == "days since 1901-01-01 00:00:00" or time_unit == "Days since 1901-01-01":
            standardized_time.append(datetime.datetime(1901,1,1) + datetime.timedelta(days=time))
        elif time_unit == "Days since 1960-01-01 00:00:00":
            standardized_time.append(datetime.datetime(1960,1,1) + datetime.timedelta(days=time))    
        else:
            raise("Error, unknown format:",time_unit)
            standardized_time.append(-9999)
    return standardized_time

def convert_netcdf_geotiff(dictje):
    """ Convert netcdf to geotiff
    
    Args:
        dictionary with root, filename and properties.
    
    """
    
    input_path = os.path.join(dictje["root"],dictje["filename"])
    nc_fid = netCDF4.Dataset(input_path, 'r')
    nc_attrs, nc_dims, nc_vars = ncdump(nc_fid)
    parameter = nc_vars[-1]
    lats = nc_fid.variables['lat'][:]  # extract/copy the data
    lons = nc_fid.variables['lon'][:]
    times = nc_fid.variables['time'][:]
    time_unit = nc_fid.variables["time"].getncattr("units")

    standardized_time = standardize_time(time_unit,times)
    
    i = 0 # single time step
    Z = nc_fid.variables[parameter][i, :, :]
    Z[Z<-9990]= -9999
    Z[Z>1e19] = -9999
    
    Z = np.flipud(Z) #depending on NetCDF type. 
    
    base_filename, extension = dictje["filename"].split(".")
    output_filename = base_filename + ".tif"    
    output_path_geotiff = os.path.join(ec2_output_path,output_filename)    
    geotransform, geoprojection = get_global_georeference(Z)    
    write_geotiff(output_path_geotiff,geotransform,geoprojection,Z,nodata_value=-9999,datatype=gdal.GDT_Float32)
    
    return standardized_time  

def pickle_dictionary(dictje):
    base_filename, extension = dictje["filename"].split(".")
    output_filename = base_filename + ".pickle"    
    output_path_pickle = os.path.join(ec2_output_path,output_filename)   
    
    with open(output_path_pickle, 'wb') as handle:
        pickle.dump(dictje, handle, protocol=pickle.HIGHEST_PROTOCOL)
    




# In[6]:

ID = 0
master_dict = {}
for root, dirs, files in os.walk(EC2_INPUT_PATH):
    for one_file in files:
        if one_file.endswith("nc"):
            file_dict = {}
            file_dict["root"] = root
            file_dict["filename"] = one_file
            filename , extension = one_file.split(".")
            file_dict["properties_from_filename"] = filename_to_dict(filename)
            master_dict[ID] = file_dict
            ID += 1


# In[7]:

# number of files that need to be converted


# In[8]:

print(len(master_dict.keys()))


# In[9]:

for ID, dictje in master_dict.items():
    master_dict[ID]["global_attributes"] = get_global_attributes(dictje)
    master_dict[ID]["variable_attributes"] = get_variable_attributes(dictje)


# In[10]:

def process_items(dictje):
    try:
        convert_netcdf_geotiff(dictje)
        pickle_dictionary(dictje)
        print(dictje["filename"])
    except:
        print("error",dictje["filename"])


# In[11]:

if TESTING:
    n = 10
    master_dict = {k: master_dict[k] for k in list(master_dict)[:n]}


# In[ ]:

cpu_count = mp.cpu_count()
p= mp.Pool(3) # Memory issues, limiting processes to 3, using appr 60-80% memory
processed_values= p.map( process_items, master_dict.values())  
p.close()
p.join()


# In[ ]:

get_ipython().system('gsutil -m cp -r {ec2_output_path} {GCS_OUTPUT_PATH}')


# In[ ]:

get_ipython().system('aws s3 cp --recursive {ec2_output_path} {s3_output_path}')


# In[ ]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous runs:  
# 2:27:01.588096
# 
