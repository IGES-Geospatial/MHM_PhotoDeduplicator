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
df = pd.concat([l,w,a], ignore_index=True)

# Combine into dataframe with one row for each observation
df = df.groupby(["MosquitoHabitatMapperId"], as_index=False).first()

# Match formatting of original fields
# Replace NA values with empty string
df = df.fillna('')
# Replace 'null' strings with empty strings
df = df.replace('null', '', regex=False)
# Add "mhm_" prefix
df = df.add_prefix('mhm_')
# Rename fields
df.rename(columns = {'mhm_MeasurementLatitude':'mhm_Latitude', 'mhm_MeasurementLongitude':'mhm_Longitude'}, inplace = True)
# Match Field Order
df = df[['mhm_protocol', 'mhm_organizationId', 'mhm_organizationName', 'mhm_siteId', 'mhm_siteName', 'mhm_ExtraData', 'mhm_AbdomenCloseupPhotoUrls', 'mhm_LarvaeCount', 'mhm_MosquitoEggs', 'mhm_LocationAccuracyM', 'mhm_MosquitoEggCount', 'mhm_Comments', 'mhm_WaterSourcePhotoUrls', 'mhm_Latitude', 'mhm_Longitude', 'mhm_MosquitoHabitatMapperId', 'mhm_BreedingGroundEliminated', 'mhm_MeasuredAt', 'mhm_MeasurementElevation', 'mhm_Userid', 'mhm_Genus', 'mhm_LocationMethod', 'mhm_WaterSource', 'mhm_MosquitoAdults', 'mhm_Species', 'mhm_MosquitoPupae', 'mhm_DataSource', 'mhm_LarvaFullBodyPhotoUrls', 'mhm_LastIdentifyStage', 'mhm_WaterSourceType', 'mhm_GlobeTeams']]

# save to CSV
csv_path = "./MHM_NoDups.csv"
df.to_csv(csv_path, sep=',', index=False, encoding='utf-8', quoting=csv.QUOTE_ALL, quotechar='"', escapechar='"')
