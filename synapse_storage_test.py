import synapseclient
#import synapseutils
#import sys
#import re
#import os
import pandas as pd
#from synapseclient import Project, File, Folder, Activity
#from synapseclient import Schema, Column, Table, Row, RowSet, as_table_columns

### login to synapse and get the flatstat file
syn = synapseclient.Synapse()
syn.login()
entity = syn.tableQuery("SELECT * FROM syn20446927 ")
files_df = entity.asDataFrame()
htanTeams =files_df.allowedTeam.unique()

dict_team_members = {}
for i in htanTeams:
    print(i)
    teamMembers = syn.getTeamMembers(i)
    dict_team_members[i] = list(teamMembers)

currentUser = syn.getUserProfile()
currentUserName = currentUser.userName


print(dict_team_members)
print(currentUserName)

# get projects a user has access to: set X

# get projects in the HTAN admin fileview syn20446927: set Y

# list projects in X intersect Y


