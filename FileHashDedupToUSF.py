#!/usr/bin/env python
# coding: utf-8

# Import Libraries
import pandas as pd
import csv

# Load "Photo First" FileHash data from Parquet file
parquet_path = './MHM_FileHashes.parquet'
df = pd.read_parquet(parquet_path)

# Drop all photos (rows) with duplicate filehashes
df.drop_duplicates(subset=['ThumbnailSHA256Hash'], keep=False, inplace=True, ignore_index=True)

# Drop "photo first" columns
df.drop(columns=['ThumbnailSHA256Hash', 'ThumbnailUrl'], inplace=True)

# Recombine photo urls into lists according to photo type - matching format of original GO data
w = df[df['PhotoType'].str.contains('WaterSourcePhoto')==True]
w = w.drop(columns=['PhotoType'])
w_map = {col: "first" for col in w.columns}
w_map["PhotoUrl"] = list
w = w.groupby(["MosquitoHabitatMapperId"], as_index=False).agg(w_map)
w.rename(columns = {'PhotoUrl':'WaterSourcePhotoUrls'}, inplace = True)
w["WaterSourcePhotoUrls"] = w["WaterSourcePhotoUrls"].str.join("; ")

l = df[df['PhotoType'].str.contains('LarvaFullBodyPhoto')==True]
l = l.drop(columns=['PhotoType'])
l_map = {col: "first" for col in l.columns}
l_map["PhotoUrl"] = list
l = l.groupby(["MosquitoHabitatMapperId"], as_index=False).agg(l_map)
l.rename(columns = {'PhotoUrl':'LarvaFullBodyPhotoUrls'}, inplace = True)
l["LarvaFullBodyPhotoUrls"] = l["LarvaFullBodyPhotoUrls"].str.join("; ")

a = df[df['PhotoType'].str.contains('AbdomenCloseupPhoto')==True]
a = a.drop(columns=['PhotoType'])
a_map = {col: "first" for col in a.columns}
a_map["PhotoUrl"] = list
a = a.groupby(["MosquitoHabitatMapperId"], as_index=False).agg(a_map)
a.rename(columns = {'PhotoUrl':'AbdomenCloseupPhotoUrls'}, inplace = True)
a["AbdomenCloseupPhotoUrls"] = a["AbdomenCloseupPhotoUrls"].str.join("; ")

#Concat into one dataframe with rows containing combined photo lists for each type
df = pd.concat([l,w,a], ignore_index=True).groupby(["MosquitoHabitatMapperId"], as_index=False).first()

# Append Date Columns from JSON API response, matched on observation ID
# Import Libraries
import requests
import json
import datetime
# Define the GLOBE API request URL
base_url = "https://api.globe.gov/search/v1/measurement/protocol/measureddate/"
requestParameters = {
    "protocols": "mosquito_habitat_mapper",
    "startdate": "2017-05-01",
    "enddate" : datetime.datetime.utcnow().date(),
    "geojson" : "FALSE", 
    "sample" : "FALSE"
}
# Make the request
response = requests.get(base_url, params=requestParameters)
# Keep the results
results = response.json()["results"]
# Pass the results as a Dataframe
df_t = pd.DataFrame(results)
# Expand the nested 'data' column by listing the contents and passing as a new dataframe
df_t = pd.concat([df_t, pd.DataFrame(list(df_t['data']))], axis=1)
#Drop the previously nested data column
df_t = df_t.drop('data', axis=1)
#Rename/Shorten Columns
df_t.columns = df_t.columns.str.replace('mosquitohabitatmapper', '')
# Keep the temporal columns and MosquitoHabitatMapperId
df_t = df_t[['MosquitoHabitatMapperId', 'measuredDate', 'createDate', 'updateDate', 'publishDate']]
# Treat all columns as strings
df_t = df_t.astype(str)

# Append temporal fields, matched on MosquitoHabitatMapperId
df = df.merge(df_t, how='inner', on='MosquitoHabitatMapperId', suffixes=(False, False))

# Match formatting of original fields
# Replace NA values with empty string
df = df.fillna('')
# Replace 'null' strings with empty strings
df = df.replace('null', '', regex=False)
# Add "mhm_" prefix
df = df.add_prefix('mhm_')
# Match USF field names
df.rename(columns = {'mhm_MeasurementLatitude':'mhm_Latitude', 'mhm_MeasurementLongitude':'mhm_Longitude'}, inplace = True)
# Match USF field order
df = df[['mhm_protocol', 'mhm_measuredDate', 'mhm_createDate', 'mhm_updateDate', 'mhm_publishDate', 'mhm_organizationId', 'mhm_organizationName', 'mhm_siteId', 'mhm_siteName', 'mhm_ExtraData', 'mhm_AbdomenCloseupPhotoUrls', 'mhm_LarvaeCount', 'mhm_MosquitoEggs', 'mhm_LocationAccuracyM', 'mhm_MosquitoEggCount', 'mhm_Comments', 'mhm_WaterSourcePhotoUrls', 'mhm_Latitude', 'mhm_Longitude', 'mhm_MosquitoHabitatMapperId', 'mhm_BreedingGroundEliminated', 'mhm_MeasuredAt', 'mhm_MeasurementElevation', 'mhm_Userid', 'mhm_Genus', 'mhm_LocationMethod', 'mhm_WaterSource', 'mhm_MosquitoAdults', 'mhm_Species', 'mhm_MosquitoPupae', 'mhm_DataSource', 'mhm_LarvaFullBodyPhotoUrls', 'mhm_LastIdentifyStage', 'mhm_WaterSourceType', 'mhm_GlobeTeams']]
# Drop any rows missing Latitude or Longitude values
df = df[df.mhm_Latitude != '']
df = df[df.mhm_Longitude != '']

# save to CSV
csv_path = "./MHM_USF.csv"
df.to_csv(csv_path, sep=',', index=False, encoding='utf-8', quoting=csv.QUOTE_ALL, quotechar='"', escapechar='"')
