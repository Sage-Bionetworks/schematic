#!/usr/bin/env python

"""
Objective: Upload local file to Google Storage
Note: Easier to wrap the simple command, the python API requires a credentials file and more setup 
Future: composite upload, crc check 

Dependencies:
install Google SDK
set up gcloud init, gcloud auth
"""

import os

filepath = '/Users/xdoan/Shell/HTAN-data-pipeline/test_data/test.txt'

# os.system("gsutil cp " + filepath + " gs://htan-data-model-testing "  )

### for larger files
### -m for multithreaded 
### parallel composite upload for files > 150MB
os.system("gsutil -o GSUtil:parallel_composite_upload_threshold=150M -m cp " + filepath + " gs://htan-data-model-testing "  )
