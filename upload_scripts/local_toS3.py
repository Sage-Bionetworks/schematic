#!/usr/bin/env python

# Copyright 2010-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# This file is licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License. A copy of the
# License is located at
#
# http://aws.amazon.com/apache2.0/
#
# This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS
# OF ANY KIND, either express or implied. See the License for the specific
# language governing permissions and limitations under the License.

"""
Objective: Upload local file to S3

Dependencies:
install install boto3, awscli, awsmfa
configure your aws default/profile with your access key/secret access key and
authenticate with awsmfa (https://sagebionetworks.jira.com/wiki/spaces/IT/pages/763199503/AWS+MFA)
"""

import boto3

# Create an S3 client
s3 = boto3.client('s3')

filepath = '/Users/xdoan/Shell/HTAN-data-pipeline/test_data/test.txt'
filename = 'test.txt'
bucket_name = 'xdoan-test-s3-synapseencryptedexternalbucket-1un999l1wbrac'

# Uploads the given file using a managed uploader, which will split up large
# files automatically and upload parts in parallel.
s3.upload_file(filepath, bucket_name, filename) ### if you don't put a filename it will create the WHOLE filepath in the filename