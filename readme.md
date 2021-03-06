# Flood Risk Inundation Data Ingestion

This repo contains scripts used to convert NetCDF files to Geotiffs and ingest them in earthengine. The results are stored in an earthengine imageCollection:
`projects/WRI-Aquaduct/floods/Y2018M08D16_RH_Floods_Inundation_EE_V01/output_V05/inundation`

Steps:

1. Copy files from S3 to EC2  
1. Convert netCDF to geotiff  
1. Upload to GCS  
1. Ingest EE  
1. Clean up

You can use the standard Aqueduct 3.0 Docker container to run the notebooks. 

`jupyter notebook --no-browser --ip=0.0.0.0 --allow-root --certfile=/.keys/mycert.pem --keyfile=/.keys/mykey.key --notebook-dir= /volumes/repos/flood_risk_analyzer_ingest/ --config=/volumes/flood_risk_analyzer_ingest/jupyter_notebook_config.py`

Explore results in Earthengine although you might need the S3 folder structure to determine sensible settings.  
https://code.earthengine.google.com/0af145cb3e2bf87aaf1eb529241fac9f





