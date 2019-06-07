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

os.system("gsutil cp " + filepath + " gs://htan-data-model-testing "  )
