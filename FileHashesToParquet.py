#!/usr/bin/env python
# coding: utf-8

# Import Libraries
import pandas as pd
import requests
import json
import datetime
import csv
import hashlib
import pyarrow

# GET MHM DATA from GLOBE API

# Define the GLOBE API request URL
base_url = "https://api.globe.gov/search/v1/measurement/protocol/measureddate/"
requestParameters = {
    "protocols": "mosquito_habitat_mapper",
    "startdate": "2017-05-01",
    "enddate" : datetime.datetime.utcnow().date(),
    "geojson" : "TRUE",
    "sample" : "FALSE"
}


# Make the request
response = requests.get(base_url, params=requestParameters)

# Normalize JSON to Dataframe
df = pd.json_normalize(response.json(), record_path=['features'])

# Drop "type" column as it is a remnant of original GeoJSON
df.drop(columns=["type"], inplace=True)

# Replace NA values with 'null' string
df = df.fillna('null')

# Shorten Column Names
df.columns = df.columns.str.replace('properties.', '', regex=False)
df.columns = df.columns.str.replace('mosquitohabitatmapper', '', regex=False)

# Keep Only measurements submitted via GLOBE Observer App
df = df[df["DataSource"]=='GLOBE Observer App']

# As we are only interested in device measurements
## Drop MGRS Coordinates
## Drop countryName, countryCode, elevation - these are calculated from MGRS Coordinates
df.drop(columns=["geometry.type","geometry.coordinates","countryCode","countryName","elevation"], inplace=True)


# Restructure Data into "Photo First" format
# Convert URL strings into Lists
df["WaterSourcePhotoUrls"] = df['WaterSourcePhotoUrls'].str.split('; ')
df["AbdomenCloseupPhotoUrls"] = df['AbdomenCloseupPhotoUrls'].str.split('; ')
df["LarvaFullBodyPhotoUrls"] = df['LarvaFullBodyPhotoUrls'].str.split('; ')

# Explode the lists and rename in photo-first approach
explode_larvae = df.explode('LarvaFullBodyPhotoUrls')
explode_larvae = explode_larvae.dropna(subset = ['LarvaFullBodyPhotoUrls'])
larvae = explode_larvae.drop(columns=['WaterSourcePhotoUrls', 'AbdomenCloseupPhotoUrls'])
larvae.rename(columns={'LarvaFullBodyPhotoUrls': 'PhotoUrl'}, inplace=True)
larvae["PhotoType"] = 'LarvaFullBodyPhoto'
larvae = larvae[larvae['PhotoUrl'].str.contains('null')==False]
larvae = larvae[larvae['PhotoUrl'].str.contains('rejected|pending approval')==False]

explode_water = df.explode('WaterSourcePhotoUrls')
explode_water = explode_water.dropna(subset = ['WaterSourcePhotoUrls'])
water = explode_water.drop(columns=['LarvaFullBodyPhotoUrls', 'AbdomenCloseupPhotoUrls'])
water.rename(columns={'WaterSourcePhotoUrls': 'PhotoUrl'}, inplace=True)
water["PhotoType"] = 'WaterSourcePhoto'
water = water[water['PhotoUrl'].str.contains('null')==False]
water = water[water['PhotoUrl'].str.contains('rejected|pending approval')==False]

explode_closeup = df.explode('AbdomenCloseupPhotoUrls')
explode_closeup = explode_closeup.dropna(subset=["AbdomenCloseupPhotoUrls"])
closeup = explode_closeup.drop(columns = ['LarvaFullBodyPhotoUrls', 'WaterSourcePhotoUrls'])
closeup.rename(columns={'AbdomenCloseupPhotoUrls': 'PhotoUrl'}, inplace=True)
closeup["PhotoType"] = 'AbdomenCloseupPhoto'
closeup = closeup[closeup['PhotoUrl'].str.contains('null')==False]
closeup = closeup[closeup['PhotoUrl'].str.contains('rejected|pending approval')==False]

#Recombine
l = larvae.reset_index(drop=True)
w = water.reset_index(drop=True)
c = closeup.reset_index(drop=True)
freshData = pd.concat([l,w,c], ignore_index=True)

# Re-Order Columns for easier visual inspection
freshData = freshData[['protocol', 'DataSource', 'MeasuredAt', 'PhotoUrl', 'PhotoType', 'WaterSourceType', 'WaterSource', 'MeasurementLatitude', 'MeasurementLongitude', 'LocationMethod', 'LocationAccuracyM', 'MosquitoHabitatMapperId', 'Userid', 'LastIdentifyStage', 'Genus', 'Species', 'MosquitoAdults', 'MosquitoPupae', 'LarvaeCount', 'MosquitoEggs', 'MosquitoEggCount', 'Comments', 'BreedingGroundEliminated', 'GlobeTeams', 'organizationId', 'organizationName', 'siteId', 'siteName', 'ExtraData', 'MeasurementElevation']]

# Create column for thumbnail image URLs - smaller size facilitates downloading and generating file hashes.
freshData.insert(0,"ThumbnailUrl", freshData["PhotoUrl"].apply(lambda x: x.rsplit('/',1)[0] + '/thumb.jpg'))

# At this stage, the most recent MHM Data from GLOBE API exists in a Pandas Dataframe named "freshData", structured with each photo as its own entry
# The next step in the process is to compare these image URLs against image URLs that have already seen, downloaded, and hashed
# The previously run process has had its output saved to a Parquet file

# Read historic data from parquet into dataframe
parquet_path = './MHM_FileHashes.parquet'
historicData = pd.read_parquet(parquet_path)
# Dataframe containing new photo urls not already in the hash db
newPhotos = freshData[~freshData["PhotoUrl"].isin(historicData["PhotoUrl"])].reset_index(drop=True)



# Function to generate FileHashes
def setSHA256Hash(url):
    response = requests.get(url)
    content = bytes(response.content)
    readable_hash = hashlib.sha256(content).hexdigest();
    return str(readable_hash)

# Generate FileHashes for new photos
newPhotos.insert(0,"ThumbnailSHA256Hash", newPhotos["ThumbnailUrl"].map(setSHA256Hash))

# Add newly hashed image data to historicData
updatedData = pd.concat([historicData, newPhotos], axis=0, ignore_index=True)

# Write to Parquet
parquet_path = './MHM_FileHashes.parquet'
updatedData = updatedData.astype('string')
updatedData.to_parquet(parquet_path)

# Write to csv
csv_path = './MHM_FileHashes.csv'
updatedData.to_csv(csv_path, sep=',', index=False, encoding='utf-8', quoting=csv.QUOTE_ALL, quotechar='"', escapechar='"')

# Write Summary Data to Summary File
summary_path = "./Summary.txt"
allImages = updatedData.copy()
duplicates = allImages[allImages.duplicated(['ThumbnailSHA256Hash'], keep=False)]
summaryString = f'''Summary of image duplication in the GLOBE MHM dataset as of {datetime.datetime.utcnow()}:
    Occurrences of image duplication: {duplicates.shape[0]}/{allImages.shape[0]}
    Number of unique images that have been duplicated: {duplicates["ThumbnailSHA256Hash"].nunique()}/{allImages["ThumbnailSHA256Hash"].nunique()}
    Number of observations containing duplicated images: {duplicates['MosquitoHabitatMapperId'].nunique()}/{allImages['MosquitoHabitatMapperId'].nunique()}
    Number of unique Userids submitting duplicated images: {duplicates['Userid'].nunique()}/{allImages['Userid'].nunique()}'''
with open(summary_path,"w+") as f:
    f.writelines(summaryString)
