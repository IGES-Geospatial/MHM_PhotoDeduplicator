# MHM_PhotoDeduplicator

It was discovered in October 2022 that there are a large number of duplicate images in the GLOBE Observer Mosquito Habitat Mapper (MHM) database. The code in this repository is intended to establish a publicly-accessible, daily-updated record of MHM images and their filehashes. It also produces a CSV record of all MHM observations that contain unique photos. The particular deduplication approach and formatting used for the output file is intended to simplify integration into the [Global Mosquito Observations Dashboard](http://mosquitodashboard.org) project maintained by the University of South Florida.

**The general structure is as follows:**
1. A running record of "photo first" data and associated filehashes reported as both .parquet and .csv: [MHM_FileHashes.parquet](https://github.com/IGES-Geospatial/MHM_PhotoDeduplicator/blob/main/MHM_FileHashes.parquet); [MHM_FileHashes.csv](https://github.com/IGES-Geospatial/MHM_PhotoDeduplicator/blob/main/MHM_FileHashes.csv)
2. A script ([FileHashesToParquet.py](https://github.com/IGES-Geospatial/MHM_PhotoDeduplicator/blob/main/FileHashesToParquet.py)) checks daily for new data being reported from the API, and for each new photo reported generates a filehash and adds an entry to the running record. This script also generates a summary of duplicated filehashes in the running record ([Summary.txt](https://github.com/IGES-Geospatial/MHM_PhotoDeduplicator/blob/main/Summary.txt))
3. After successful execution of the above script, another script ([FileHashDedupToUSF.py](https://github.com/IGES-Geospatial/MHM_PhotoDeduplicator/blob/main/FileHashDedupToUSF.py)) runs which outputs a csv file ([MHM_USF.csv](https://github.com/IGES-Geospatial/MHM_PhotoDeduplicator/blob/main/MHM_USF.csv)) mimicking the "observation first" data reported by the API, but with all instances of photo duplication removed. **Observations without any photos, or with only duplicated photos, are omitted entirely**.

Execution is via scheduled Github Action Workflow, daily at 13:00 UTC

Further analysis and reporting on the issue of photo duplication in the MHM dataset is planned. Interested parties may conduct their own analysis or recombination approaches on the MHM_FileHashes datasets without needing to download all images before getting started. Suggested additional recombination approaches include:
+ Adding flags to indicate that a duplicate image was removed from an observation
+ Flags to indicate the type of duplication that was removed from the observation:
  + duplication of the same image within a single observation
  + duplication of the same image across multiple observations
  + duplication of the same image within a single photo type (WaterSourcePhoto, LarvaFullBodyPhoto, AbdomenCloseupPhoto)
  + duplication of the same image across different photo types
+ Column to indicate the running total count of duplicate photos submitted by the user (UserID) responsible for each observation 
+ Preserving the first instance of a photo that was later duplicated
+ Preserving all observations, even if no unique photos are present

Exploratory notebook with interactive stepwise execution via [![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/IGES-Geospatial/JupyterLabBinder/HEAD?urlpath=lab/tree/MHM_DuplicatePhotoNotebook/MHM_DuplicatePhotoNotebook.ipynb)

*More information about the GLOBE Dataset can be found in the [GLOBE Data User Guide v2.0](https://www.globe.gov/documents/10157/2592674/GLOBE+Data+User+Guide_v2_final.pdf).*

*These data were obtained from NASA and the GLOBE Program and are freely available for use in research, publications and commercial applications. When data from GLOBE are used in a publication, we request this acknowledgment be included: "These data were obtained from the GLOBE Program." Please include such statements, either where the use of the data or other resource is described, or within the Acknowledgements section of the publication.*
