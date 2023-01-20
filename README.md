# MHM_PhotoDeduplicator

It was discovered in October 2022 that there are a large number of duplicate images in the GLOBE Observer Mosquito Habitat Mapper database. The code in this repository is intended to establish a publicly-accessible, daily-updated dataset with all duplicated photos removed.

**The general structure is as follows:**
1. A running record of "photo first" data and associated filehashes reported as both .parquet and .csv: [MHM_FileHashes.parquet](https://github.com/IGES-Geospatial/MHM_PhotoDeduplicator/blob/main/MHM_FileHashes.parquet); [MHM_FileHashes.csv](https://github.com/IGES-Geospatial/MHM_PhotoDeduplicator/blob/main/MHM_FileHashes.csv)
2. A script ([FileHashesToParquet.py](https://github.com/IGES-Geospatial/MHM_PhotoDeduplicator/blob/main/FileHashesToParquet.py)) checks daily for new data being reported from the API, and for each new photo reported generates a filehash and adds an entry to the running record. This script also generates a summary of duplicated filehashes in the running record ([Summary.txt](https://github.com/IGES-Geospatial/MHM_PhotoDeduplicator/blob/main/Summary.txt))
3. After successful execution of the above script, another script ([FileHashDedupToCSV.py](https://github.com/IGES-Geospatial/MHM_PhotoDeduplicator/blob/main/FileHashDedupToCSV.py)) runs which outputs a csv file ([MHM_NoDups.csv](https://github.com/IGES-Geospatial/MHM_PhotoDeduplicator/blob/main/MHM_NoDups.csv)) mimicking the "observation first" data reported by the API, but with all instances of photo duplication removed

Execution is via scheduled Github Action Workflow, daily at 13:00 UTC

Further analysis and reporting on the issue of photo duplication in the MHM dataset is planned. Interested parties may conduct their own analysis on the MHM_FileHashes datasets without needing to download all images before getting started.

*These data were obtained from NASA and the GLOBE Program and are freely available for use in research, publications and commercial applications. When data from GLOBE are used in a publication, we request this acknowledgment be included: "These data were obtained from the GLOBE Program." Please include such statements, either where the use of the data or other resource is described, or within the Acknowledgements section of the publication.*
