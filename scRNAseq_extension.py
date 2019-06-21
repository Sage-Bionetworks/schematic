import os
import json
  
from schema_explorer import SchemaExplorer

def get_class(class_name, description = None, subclass_of = ["Thing"], requires_dependencies = None, requires_value = None):

    class_attributes = {
                    '@id': 'bts:'+class_name,
                    '@type': 'rdfs:Class',
                    'rdfs:comment': description if description else "",
                    'rdfs:label': class_name,
                    'schema:isPartOf': {'@id': 'http://schema.biothings.io'}
    }

    if subclass_of:
        parent = {'rdfs:subClassOf':[{'@id':'bts:' + sub} for sub in subclass_of]}
        class_attributes.update(parent)

    if requires_dependencies:
        requirement = {'rdfs:requiresDependency':[{'@id':'sms:' + dep} for dep in requires_dependencies]}
        class_attributes.update(requirement)

    if requires_value != None:
        value_constraint = {'rdfs:requiresChildAsValue':{'@id':'sms:' +  str(requires_value)}}
        class_attributes.update(value_constraint)
    
    return class_attributes



# path to schema metadata (output or input)
schema_path = "./schemas"
output_schema_name = "scRNASeq"


# instantiate schema explorer
se = SchemaExplorer()


"""
######################################################
# first add the classes w/o dependencies to the schema
######################################################
"""


'''
adding children classes to the Biosample class in biothing
'''

class_req_add = get_class("BiosampleType",\
                              description = "The type of source material for the biosample",\
                              subclass_of = ["Biosample"],
                              requires_value = True
)
se.update_class(class_req_add)


class_req_add = get_class("BiosampleID",\
                              description = "Unique identifier for the biosample source material",\
                              subclass_of = ["Biosample"]
)
se.update_class(class_req_add)


'''
adding children nodes of BiosampleType
'''
class_req_add = get_class("Donor",\
                              description = "A role which inheres in an organism or part thereof from which any part including cell, organ or tissue is removed with the intention that the donated part will be placed into another organism and/or cultured in vitro.<OBI_1110087>",\
                              subclass_of = ["BiosampleType"]
)
se.update_class(class_req_add) # note: use update_class to add a new class

class_req_add = get_class("Specimen",\
                              description = "A part of a thing, or of several things, taken to demonstrate or to determine the character of the whole, e.g. a substance, or portion of material obtained for use in testing, examination, or study; particularly, a preparation of tissue or bodily fluid taken for examination or diagnosis.<NCIT_C19157>",\
                              subclass_of = ["BiosampleType"]
)
se.update_class(class_req_add) # note: use update_class to add a new class


class_req_add = get_class("Organoid",\
                              description = "An artificially grown mass of cells or tissue that resembles an organ.",\
                              subclass_of = ["BiosampleType"]
)
se.update_class(class_req_add) # note: use update_class to add a new class


class_req_add = get_class("CellSuspension",\
                              description = "Particles floating in (not necessarily on) a liquid medium, or the mix of particles and liquid itself.< An artificially grown mass of cells or tissue that resembles an organ.<BTO_0000221>",\
                              subclass_of = ["BiosampleType"]
)
se.update_class(class_req_add) # note: use update_class to add a new class


class_req_add = get_class("CellLine",\
                              description = " A cultured cell population that represents a genetically stable and homogenous population of cultured cells that shares a common propagation history (i.e. has been successively passaged together in culture).<CLO_0000031>",\
                              subclass_of = ["BiosampleType"]
)
se.update_class(class_req_add) # note: use update_class to add a new class


'''
adding a generic category assay
'''
class_req_add = get_class("Assay",\
                              description = "A planned process with the objective to produce information about the material entity that is the evaluant, by physically examining it or its proxies.<OBI_0000070>"
)
se.update_class(class_req_add) # note: use update_class to add a new class


'''
adding a subcategory sequencing assay (as a subclass of assay)
'''
class_req_add = get_class("SequencingAssay",\
                              description = "The determination of the sequence of component residues in a macromolecule, e.g. amino acids in a protein or nucleotide bases in DNA/RNA or the computational analysis performed to determine the similarities between nonidentical proteins or molecules of DNA or RNA.<NCIT_C17565>",\
                              subclass_of = ["Assay"]
)
se.update_class(class_req_add) # note: use update_class to add a new class



'''
adding various classes related to (and required in the description of) sequencing assays
'''
class_req_add = get_class("NucleicAcidSource",\
                              description = "The source of the input nucleic molecule",\
                              subclass_of = ["SequencingAssay"],
                              requires_value = True
)
se.update_class(class_req_add) # note: use update_class to add a new class

'''
adding children to NucleicAcidSource (the range of possible values NucleicAcidSource can take on"
'''
class_req_add = get_class("BulkCell",\
                              description = "A collection of cells",\
                              subclass_of = ["NucleicAcidSource"]
)
se.update_class(class_req_add) # note: use update_class to add a new class

class_req_add = get_class("SingleCell",\
                              description = "A cell",\
                              subclass_of = ["NucleicAcidSource"]
)
se.update_class(class_req_add) # note: use update_class to add a new class

class_req_add = get_class("BulkNuclei",\
                              description = "Nuclei from a collection of cells",\
                              subclass_of = ["NucleicAcidSource"]
)
se.update_class(class_req_add) # note: use update_class to add a new class

class_req_add = get_class("SingleNucleus",\
                              description = "Nucleus from a single cell",\
                              subclass_of = ["NucleicAcidSource"]
)
se.update_class(class_req_add) # note: use update_class to add a new class

'''
adding a subclass of sequencing assay
'''

class_req_add = get_class("LibraryConstructionMethod",\
                              description = "Process which results in the creation of a library from fragments of DNA using cloning vectors or oligonucleotides with the role of adaptors <OBI_0000711>",\
                              subclass_of = ["SequencingAssay"],
                              requires_value = True
)
se.update_class(class_req_add) # note: use update_class to add a new class

'''
adding children of library construction method
'''

class_req_add = get_class("10x",\
                              description = "10X is a 'synthetic long-read' technology and works by capturing a barcoded oligo-coated gel-bead and 0.3x genome copies into a single emulsion droplet, processing the equivalent of 1 million pipetting steps. Successive versions of the 10x chemistry use different barcode locations to improve the sequencing yield and quality of 10x experiments.<EFO_0008995>",\
                              subclass_of = ["LibraryConstructionMethod"]
)
se.update_class(class_req_add) # note: use update_class to add a new class

class_req_add = get_class("Smart-seq2",\
                              description = "Switch mechanism at the 5’ end of RNA templates (Smart).<EFO_0008930>",\
                              subclass_of = ["LibraryConstructionMethod"]
)
se.update_class(class_req_add) # note: use update_class to add a new class


'''
adding a subclass of sequencing assay
'''
class_req_add = get_class("LibraryLayout",\
                              description = "Sequencing read type"
,\
                              subclass_of = ["SequencingAssay"],
                              requires_value = True
)
se.update_class(class_req_add) # note: use update_class to add a new class


'''
adding children to library layout
'''
class_req_add = get_class("PairedEnd",\
                              description = "Sequencing DNA from paired-end tags (PET), the short sequences at the 5 prime and 3 prime ends of the DNA fragment of interest, which can be a piece of genomic DNA or cDNA.<MI_1181>"
,\
        subclass_of = ["LibraryLayout"]
)
se.update_class(class_req_add) # note: use update_class to add a new class

class_req_add = get_class("SingleRead",\
                              description = "Sequencing DNA from only one end"
,\
        subclass_of = ["LibraryLayout"]
)
se.update_class(class_req_add) # note: use update_class to add a new class


'''
adding a subclass of sequencing assay
'''
class_req_add = get_class("Primer",\
                              description = "An oligo to which new deoxyribonucleotides can be added by DNA polymerase. <SO_0000112>"
,\
        subclass_of = ["SequencingAssay"]
)
se.update_class(class_req_add) # note: use update_class to add a new class


'''
adding a subclass of sequencing assay
'''
class_req_add = get_class("Platform",\
                              description = "A platform is an object_aggregate that is the set of instruments and software needed to perform a process. <OBI_0000050>"
,\
        subclass_of = ["SequencingAssay"],
        requires_value = True
)
se.update_class(class_req_add) # note: use update_class to add a new class



'''
adding avarious platforms as children to platform
'''
class_req_add = get_class("IlluminaiNextSeq500",\
                              description = "sequencing platform"
,\
        subclass_of = ["Platform"]
)
se.update_class(class_req_add) # note: use update_class to add a new class

class_req_add = get_class("IlluminaiNextSeq2500",\
                              description = "sequencing platform"
,\
        subclass_of = ["Platform"]
)
se.update_class(class_req_add) # note: use update_class to add a new class



'''
adding a scRNASeq as a subclass of sequencing assay
'''
class_req_add = get_class("scRNASeq",\
                              description = "Single-cell RNA-seq."
,\
        subclass_of = ["SequencingAssay"]
)
se.update_class(class_req_add) # note: use update_class to add a new class

'''
adding a generic category protocol
'''
class_req_add = get_class("Protocol",\
                              description = "A plan specification which has sufficient level of detail and quantitative information to communicate it between investigation agents, so that different investigation agents will reliably be able to independently reproduce the process.<OBI_0000272>",\
                              subclass_of = ["Thing"]
)
se.update_class(class_req_add) # note: use update_class to add a new class

'''
adding protocol classes
'''

class_req_add = get_class("SpikeIn",\
                              description = "An RNA spike-in is an RNA transcript of known sequence and quantity used to calibrate measurements in RNA hybridization assays.",\
                              subclass_of = ["Protocol"],\
                              requires_value = True
)
se.update_class(class_req_add) # note: use update_class to add a new class

'''
adding children to spike in (restricting possible value range
'''
class_req_add = get_class("ERCC",\
                              description = "A common set of external RNA controls that has been developed by the External RNA Controls Consortium",\
                              subclass_of = ["SpikeIn"]
)
se.update_class(class_req_add) # note: use update_class to add a new class

class_req_add = get_class("OtherSpikeIn",\
                              description = "Other spike-in controls",\
                              subclass_of = ["SpikeIn"]
)
se.update_class(class_req_add) # note: use update_class to add a new class

class_req_add = get_class("NoSpikeIn",\
                              description = "No spike-in controls added",\
                              subclass_of = ["SpikeIn"]
)
se.update_class(class_req_add) # note: use update_class to add a new class


class_req_add = get_class("SingleCellIsolation",\
                              description = "A plan specification which has sufficient level of detail and quantitative information to communicate it between investigation agents, so that different investigation agents will reliably be able to independently reproduce the process.<OBI_0000272>",\
                              subclass_of = ["Protocol", "scRNASeq"],
                              requires_value = True
)
se.update_class(class_req_add) # note: use update_class to add a new class

'''
adding children to single cell isolation (constraining values)
'''
class_req_add = get_class("Droplets",\
                              description = "description pending",\
                              subclass_of = ["SingleCellIsolation"]
)
se.update_class(class_req_add) # note: use update_class to add a new class

class_req_add = get_class("FACS",\
                              description = "description pending",\
                              subclass_of = ["SingleCellIsolation"]
)
se.update_class(class_req_add) # note: use update_class to add a new class

class_req_add = get_class("Microfluidics chip",\
                              description = "description pending",\
                              subclass_of = ["SingleCellIsolation"]
)
se.update_class(class_req_add) # note: use update_class to add a new class



'''
adding a protocol category as a subclass
'''

class_req_add = get_class("ReverseTranscription",\
                              description = "A DNA synthesis process that uses RNA as the initial template for synthesis of DNA, but which also includes an RNase activity to remove the RNA strand of an RNA-DNA heteroduplex produced by the RNA-dependent synthesis step and use of the initial DNA strand as a template for DNA synthesis. <GO_0001171>",\
                              subclass_of = ["Protocol"],
                              requires_value = True
)
se.update_class(class_req_add) # note: use update_class to add a new class

'''
adding children to reverse transcription (restricting possible range of options)
'''
class_req_add = get_class("CellBarcode",\
                              description = "description pending",\
                              subclass_of = ["ReverseTranscription"]
)
se.update_class(class_req_add) # note: use update_class to add a new class

class_req_add = get_class("UMI",\
                              description = "description pending",\
                              subclass_of = ["ReverseTranscription"]
)

se.update_class(class_req_add) # note: use update_class to add a new class

class_req_add = get_class("UMIandCellBarcode",\
                              description = "description pending",\
                              subclass_of = ["ReverseTranscription"]
)
se.update_class(class_req_add) # note: use update_class to add a new class


'''
adding classes dependent on reverse transcription children (note this does not create requirement dependencies automatically; the requirement dependencies will be specifically added further below)
'''

class_req_add = get_class("CellBarcodeRead",\
                              description = "description pending",\
                              subclass_of = ["CellBarcode"]
)
se.update_class(class_req_add) # note: use update_class to add a new class

class_req_add = get_class("UMIBarcodeRead",\
                              description = "description pending",\
                              subclass_of = ["UMI"]
)
se.update_class(class_req_add) # note: use update_class to add a new class

'''
adding a protocol subclass
'''

class_req_add = get_class("Amplification",\
                              description = "Addition of extra material.<NCIT_C25418>",\
                              subclass_of = ["Protocol"],
                              requires_value = True
)
se.update_class(class_req_add) # note: use update_class to add a new class


'''
adding children to amplification (restricting possible range of options)
'''
class_req_add = get_class("PCR",\
                              description = "PCR is the process in which a DNA polymerase is used to amplify a piece of DNA by in vitro enzymatic replication. As PCR progresses, the DNA thus generated is itself used as a template for replication. This sets in motion a chain reaction in which the DNA template is exponentially amplified.<OBI_0000415>",\
                              subclass_of = ["Amplification"]
)
se.update_class(class_req_add) # note: use update_class to add a new class

class_req_add = get_class("InVitroTranscription",\
                              description = "Biomolecule synthesis of RNA in vitro used for applications such as can be used in blot hybridizations and nuclease protection assays.<ERO_0000905>",\
                              subclass_of = ["Amplification"]
)
se.update_class(class_req_add) # note: use update_class to add a new class


'''
adding a new generic category raw data
'''

class_req_add = get_class("RawData",\
                              description = "Data generated as direct output of assay platforms",\
                              subclass_of = ["Thing"]
)
se.update_class(class_req_add)



'''
adding classes related to raw data attributes
'''
class_req_add = get_class("Filename",\
                              description = " The literal identifier of a file that is not located within a specific record.<NCIT_C82536>",\
                              subclass_of = ["RawData"]
)
se.update_class(class_req_add)

class_req_add = get_class("AssayType",\
                              description = "The type of assay pertaining to a dataset",\
                              subclass_of = ["RawData", "Assay"]
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
# edit class scRNASeq to add example dependency requirements
i.e. any scRNASeq thing (e.g. data file) requires dependencies/annotations 
"BiosampleType", "BiosampleID", "NucleicAcidSoure", etc.

note that these dependencies can but do not need to be children of scRNASeq
"""
class_info = se.explore_class("scRNASeq")
class_req_edit = get_class("scRNASeq",\
                              description = class_info["description"],\
                              subclass_of = class_info["subClassOf"],\
                              requires_dependencies = ["BiosampleType", "BiosampleID", "NucleicAcidSource", "LibraryConstructionMethod", "LibraryLayout", "Primer", "Platform", "SingleCellIsolation", "SpikeIn", "ReverseTranscription", "Amplification", "Filename"]
)
se.edit_class(class_req_edit) 
# note: use class_edit to edit a current class; you can explore existing class properties
# and reuse them in the edited class (e.g. here via class_info)


class_info = se.explore_class("UMI")
class_req_edit = get_class("UMI",\
                              description = class_info["description"],\
                              subclass_of = class_info["subClassOf"],\
                              requires_dependencies = ["UMIBarcodeRead"]
)
se.edit_class(class_req_edit) 


class_info = se.explore_class("UMIandCellBarcode")
class_req_edit = get_class("UMIandCellBarcode",\
                              description = class_info["description"],\
                              subclass_of = class_info["subClassOf"],\
                              requires_dependencies = ["UMIBarcodeRead", "CellBarcodeRead"]
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
json_schema_name = "scRNASeqJSONSchema"

json_schema = get_JSONSchema_requirements(se, "scRNASeq", schema_name = json_schema_name)

# store the JSONSchema schema
with open(os.path.join(schema_path, json_schema_name + ".json"), "w") as s_f:
    json.dump(json_schema, s_f, indent = 3) # adjust indent - lower for more compact schemas
