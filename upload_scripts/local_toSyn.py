#!/usr/bin/env python

"""
Objective: Upload local file to Synapse
Future: No metadata as of now but using synceToSynapse for the future metadata

Dependencies:
install https://github.com/Sage-Bionetworks/synAnnotationUtils
make sure you have a synapseConfig
"""
import os
import synapseclient
from synapseclient import Entity, Project, Folder, File, Link
import synapseutils

syn = synapseclient.Synapse()
syn.login()

test_file = synapseclient.entity.File( '~/Shell/HTAN-data-pipeline/test_data/test.txt', parent = 'syn18906689')

# test_manifest = synapseutils.sync.generateManifest(syn, test_file, "test_manifest.tsv")

os.system("python ~/Shell/synAnnotationUtils/bin/sync_manifest.py -d ~/Shell/HTAN-data-pipeline/test_data/ --id syn18906689 > ~/Shell/HTAN-data-pipeline/test_manifest/test_manifest.tsv "  )

test_manifest = "~/Shell/HTAN-data-pipeline/test_manifest/test_manifest.tsv"

synapseutils.sync.syncToSynapse(syn, test_manifest, dryRun= False)