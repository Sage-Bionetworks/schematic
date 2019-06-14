import os
import json
  
from schema_explorer import SchemaExplorer

def get_class(class_name, description = None, subclass_of = "Thing", requires_dependencies = None, requires_value = None):

    class_attributes = {
                    '@id': 'bts:'+class_name,
                    '@type': 'rdfs:Class',
                    'rdfs:comment': description if description else "",
                    'rdfs:label': class_name,
                    'rdfs:subClassOf': {'@id': 'bts:' + subclass_of},
                    'schema:isPartOf': {'@id': 'http://schema.biothings.io'}
    }

    if requires_dependencies:
        requirement = {'rdfs:requiresDependency':[{'@id':'sms:' + dep} for dep in requires_dependencies]}
        class_attributes.update(requirement)

    if requires_value != None:
        value_constraint = {'rdfs:requiresChildAsValue':{'@id':'sms:' +  str(requires_value)}}
        class_attributes.update(value_constraint)
    
    return class_attributes



# path to schema metadata (output or input)
schema_path = "./schemas"
output_schema_name = "exampleSchemaReq"


# instantiate schema explorer
se = SchemaExplorer()


"""
######################################################
# first add the classes w/o dependencies to the schema
######################################################
"""

'''
adding fileFormat as a child of Thing
'''
class_req_add = get_class("fileFormat",\
                              description = "Defined format of the data file, typically corresponding to extension, but sometimes indicating more general group of files produced by the same tool or software",\
                              subclass_of = "Thing"
)
se.update_class(class_req_add)


'''
adding resourceType as a child of Thing
'''
class_req_add = get_class("resourceType",\
                              description = "The type of resource being stored and annotated",\
                              subclass_of = "Thing"
)
se.update_class(class_req_add) # note: use update_class to add a new class

'''
# adding two children to resourceType:
    - experimentalData
    - tool
'''
class_req_add = get_class("experimentalData",\
                              description = "Any file derived from or pertaining to a scientific experiment. experimentalData annotations should be applied, possibly disease-related",\
                              subclass_of = "resourceType"
)
se.update_class(class_req_add)


class_req_add = get_class("tool",\
                              description = "Software code or artifact tool",\
                              subclass_of = "resourceType"
)
se.update_class(class_req_add)


'''
adding class assay as a subclass of Biosample
'''
class_req_add = get_class("assay",\
                              description = "The technology used to generate the data for this Biosample",\
                              subclass_of = "Biosample"
)
se.update_class(class_req_add)

'''
adding specimenID as a subclass of Biosample
'''
class_req_add = get_class("specimenID",\
                              description = "Identifying string linked to a particular sample or specimen",\
                              subclass_of = "Biosample"
)
se.update_class(class_req_add)

'''
adding softwareName as a subclass of tool
'''
class_req_add = get_class("softwareName",\
                              description = "Name of software",\
                              subclass_of = "tool"
)
se.update_class(class_req_add)

"""
######################################################
# DONE adding classes to schema
######################################################
"""


"""
######################################################
# add dependencies and requirements to classes
######################################################
"""


"""
# edit class Thing to add example dependency requirements
i.e. any Thing (e.g. data file) requires dependencies/annotations 
"resourceType", "fileFormat"

note that these dependencies can but do not need to be children of Thing
"""
class_info = se.explore_class("Thing")
class_req_edit = get_class("Thing",\
                              description = class_info["description"],\
                              subclass_of = "Thing",\
                              requires_dependencies = ["resourceType", "fileFormat"]
)
se.edit_class(class_req_edit) 
# note: use class_edit to edit a current class; you can explore existing class properties
# and reuse them in the edited class (e.g. here via class_info)


"""
#  edit class resourceType with example value-range requirement

in this case the requirement is that resourceType should be annotated 
with specific values. requires_value translates to requiresChildAsValue;
this is configurable, however the current interpretation is that the
value resourceType can be annotated with should match the label of one of its
children: experimentalData and tool, classes added above. 

Hence given the requirements of resourceType above, resourceType can be set to
either experimentalData or tool; note that each of these children could have their own
requirements (both in terms of value range and other dependencies); for instance,
experimentalData requires assay, specimenID (e.g. as data annotations);
similarly tool requires softwareName; see below
"""
class_info = se.explore_class("resourceType")
class_req_edit = get_class("resourceType",\
                              description = class_info["description"],\
                              subclass_of = class_info["subClassOf"],\
                              requires_value = True
)
se.edit_class(class_req_edit) 


"""
#  edit class experimentalData to add dependency requirements
(e.g. required annotations, given an experimentalData thing (i.e. resourceType))
"""
class_info = se.explore_class("experimentalData")
class_req_edit = get_class("experimentalData",\
                              description = class_info["description"],\
                              subclass_of = class_info["subClassOf"],\
                              requires_dependencies = ["assay", "specimenID"] 
)
se.edit_class(class_req_edit) 


"""
#  edit class tool to add dependency requirements
(e.g. required annotations, given a tool thing (i.e. resourceType))
"""
class_info = se.explore_class("tool")
class_req_edit = get_class("tool",\
                              description = class_info["description"],\
                              subclass_of = class_info["subClassOf"],\
                              requires_dependencies = ["softwareName"] 
)
se.edit_class(class_req_edit) 

"""
#  suppose we want to constraint the types of file formats that can be accepted

One representation of the contraint would involve
 1) add the corresponding file formats to the schema (e.g. as children to fileFormat)
 2) add the value-range constraint
"""
# adding file formats
class_req_add = get_class("txt",\
                              description = "Plain text file format",\
                              subclass_of = "fileFormat"
)
se.update_class(class_req_add)

class_req_add = get_class("fastq",\
                              description = "Raw sequencing file",\
                              subclass_of = "fileFormat"
)
se.update_class(class_req_add)

# adding value-range constraint on fileFormat by editing the existing class
class_info = se.explore_class("fileFormat")
class_req_edit = get_class("fileFormat",\
                              description = class_info["description"],\
                              subclass_of = class_info["subClassOf"],\
                              requires_value = True
)
se.edit_class(class_req_edit) 

"""
######################################################
# DONE adding requirements to schema
######################################################
"""

# saving updated schema.org schema
se.export_schema(os.path.join(schema_path, output_schema_name + ".jsonld"))


"""
######################################################
# Generating JSONSchema schema from schema.org schema
######################################################
"""

'''
To generate JSONSchema schema for validation based on this schema.org schema
run ./schema_generator.py; just point it to the output schema above or invoke 
directly the JSONSchema generation method as show below
'''

from schema_generator import get_JSONSchema_requirements 

# see schema_generator.py for more details on parameters

#JSONSchema name 
json_schema_name = "exampleJSONSchema"

json_schema = get_JSONSchema_requirements(se, "Thing", schema_name = json_schema_name)

# store the JSONSchema schema
with open(os.path.join(schema_path, json_schema_name + ".json"), "w") as s_f:
    json.dump(json_schema, s_f, indent = 3) # adjust indent - lower for more compact schemas



"""
######################################################
# Generating annotations manifest from schema.org schema
######################################################
"""

from manifest_generator import get_manifest

print("==========================")
print("Generating manifest...")
print("==========================")

manifest_url = get_manifest(se, "Thing", "exampleManifest")
print(manifest_url)
