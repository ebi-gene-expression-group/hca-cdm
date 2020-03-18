# HCA to Atlas Metadata Converter

HCA metadata into Atlas Common Model python objects. Used by https://github.com/ebi-gene-expression-group/common-datamodel to write and validate MAGE-TAB.

## Setup
In a new directory run the following:

`virtualenv -p python3 venv`

`source venv/bin/activate`

`pip3 install git+https://github.com/ebi-gene-expression-group/hca-cdm.git`

`pip3 install git+https://github.com/ebi-gene-expression-group/common-datamodel`

`pip install pandas`

`it clone https://github.com/ebi-gene-expression-group/usi-arrayexpress`

`echo $PWD/usi-arrayexpress > $PWD/venv/lib/python3.7/site-packages/pythonpth`

`export PYTHONPATH="${PYTHONPATH}:$PWD/usi-arrayexpress"`

Now you are ready to use the converter. To run you need to make a python script like this example. Use this as a template and change the project uuid as required. Run with

`python3 test.py`

Example of `test.py`:
```
import logging
from os import path
import sys
from hcacdm.ProjectImporter import convert
from converter import dm2magetab

# convert a hca project to common data model object
hca_project_uuid = 'c4077b3c-5c98-4d26-a614-246d12c2e5d7'
translation_config_file = 'https://raw.githubusercontent.com/ebi-gene-expression-group/common-datamodel/mg-scisolation/datamodel/config/datamodel_mapping_config.json'
submission_object = convert(hca_project_uuid, translation_config_file)
with open("submission_object","w+") as f:
	f.write(str(submission_object.assay))

# Set the new file names

prefix = "HCA_" + hca_project_uuid
if not submission_object.info.get("metadata"):
    submission_object.info["metadata"] = prefix
outpath = "."


idf = dm2magetab.generate_idf(submission_object)
sdrf = dm2magetab.generate_sdrf(submission_object)

new_idf_file = path.join(outpath, prefix + ".idf.txt")
new_sdrf_file = path.join(outpath, prefix + ".sdrf.txt")

# Write out a new IDF file
dm2magetab.write_idf_file(idf, new_idf_file, logging.getLogger())

# Rename the columns to the new header list, created by applying a function
# to "de-uniquify" the header fields, and write new SDRF file
dm2magetab.write_sdrf_file(sdrf, new_sdrf_file, logging.getLogger())
```

## Config

There has been lots of work to map metadata attributes. Mapping requires more than one-to-one attribute mapping between the two schemas. We also need to facilitate the following:
The stoichiometry of entities is not equivalent. For example, HCA may have many separate sequencing protocols but these should be reduced to one entity for the SCEA. Or HCA may define several sub sample entities that must be combined into one sample entity for SCEA.
Value transformations are required. Sometimes each schema defines a list of enum values. These need to be mapped and translated in order to produce valid metadata.
Missing metadata. Some values are not mappable but are ‘required’ by SCEA. This necessitates bespoke handling to create the required value.
Many to one. Multiple fields may be required to map to one field. Again for these instances bespoke handling is required.

Despite the complex conversion and bespoke handling we also needed the ability to frequently update and remap entities as both schemas evolve. To do this we used a shared config that links entities in the common data model to HCA entities. The same config file also links common data model entities to SCEA and to USI/DSP. This allows us to convert between formats. This config file is good documentation about how we map entied and how values are transformed.

The config file is here https://github.com/ebi-gene-expression-group/common-datamodel/blob/master/datamodel/config/datamodel_mapping_config.json

The bespoke handling functions defined by "method" all live together in the converter here
https://github.com/ebi-gene-expression-group/hca-cdm/blob/master/hcacdm/convert_entity.py

The config file is designed to make simple updates to functionality easy to implement.

### Editing the config

The config is part of the common data model repo found here 'https://raw.githubusercontent.com/ebi-gene-expression-group/common-datamodel/master/datamodel/config/datamodel_mapping_config.json'

Config allows:
- mapping attributes
- mapping entities
- succinct special handling functions
- handling nested entities
- adding links via entity alias

Each Atlas attribute is nested under it's respective entity type. Each attribute has entiries for their respective mapping source. Currently ae (aka Array Express) and hca (aka Human Cell Atlas DCP) are supported. Under the 'hca' label each attribute has several elements described below.

An example of one attribute's mapping in the config 
```
"hca": {
  "path": [
    "dissociation_protocol_json",
    "method",
    "ontology_label"
  ],
  "from_type": "string",
  "method": "use_translation",
  "translation" : {
    "fluorescence-activated cell sorting" : "FACS",
    "10X v2 sequencing" : "null",
    "enzymatic dissociation" : "enzymatic dissociation",
    "mechanical dissociation" : "mechanical dissociation",
    "None" : "null"
  }
} 

```
#### path

HCA attributes have a 'programmatic name' described in their [metadata schema's](https://github.com/HumanCellAtlas/metadata-schema/tree/master/json_schema/type). This 'dot notation' offers a path to the nested attribute from the top level of the metadata files. This config needs to know this path in order to map attributes.

The top level is often terminated by '_json'. This is the name of the metadata document from the data store and is alwasy the top level. Nested HCA entities do not follow this pattern as their path is not HCA entity specific and they can be found in different entities. Protocols are also found in their own specific documents so may also not follow this pattern. Therefore, the path may or may not contain these top level entries at index 0.

Some special handling functions do not require a path. In these cases an empty list should be entered as this field is 'required'.

Data in the DCP data store may be stored at different versions of the metadata schema therefore this path mad differ as the schema's migrate. Therefore, the config file must be updated prior to conversion of any dataset in the datastore. Tooling will be made available to migrate the config file.

#### from_type

This term will later be used for validation of the returned metadata types. It is not used at present. 

#### method

This is a 'required' field as it dictates the method to follow. There are many reusible methods in the importer that can be reused. Special handling may require the addition of functions which can be added to the class 'get_common_model_entity_metadata.py'. Your new function should return the value to be passed to the output. The class has many constructs of the HCA's metadata objects that are useful to make these functions. To access metadata consider using 'metadata_files', 'metadata_files_by_uuid'and  'bundle_graph'. There are also several other elements to the class that capture the context of the conversion reading the translation file and gathering linking entities.

The most frequently used method is 'import_string' and 'import_string_from_selected_entity'. Both follow the path to directly map HCA attributes to the Common Data Model attributes. The latter must be passed a specific entity and can therefore be used to iterate through multiple HCA entities with the help of a preceding function.

#### translation

This dictionary is used for value manipulation as demonstrated in the example. As JSON does not allow None 'null' should be used here. This will be converted to None by the script. For functions that use the translation dictionary errors will occur if a value is found that is not mapped. This design ensures that the operator is made aware of all metadata value manipulation.

The method 'use_translation' in addition to a string (rather than a dictionary) will return that string as the value for this field. This is useful for placeholders or instances where values can be predetermined.

For example, in the context of the HCA:

library_selection = 'cDNA'
library_source = 'transcriptomic single cell'
library_strategy = 'RNA-Seq'

## Project development info

This converter relies on an endpoint (the bundles endpoint) provided by the DCP datastore that may no longer be available in DCP v2. The HCA-SCEA converter was finished in Dec 2019 and development was frozen when DCP v2 was announced.

The endpoint can only match and return assays but each assay contains all the upstream (derived from) metadata for the project. So project information, donor information etc is duplicated but information about separate assays are located in separate assay bundles. Consolidating the metadata added considerable complexity to the code but this will likely improve when the DCP offers endpoints with more functionality.

Although this work could be adapted to work in reverse it was never intended to be used that way. Therefore, the metadata transformation may not be reversible and some data may be lost in the conversion process.
Structure Overview
This converter does not map directly from DCP bundle JSON to SCEA IDF and SDRF MAGE-TAB (SCEA metadata). Another project in our group is working to produce SCEA metadata from DSP (formerly USI) JSON. To integrate with this effort the converter turns bundle JSON into a DSP based python object resembling DSP JSON schema requirements. This object (common data model) can be fed into the new SCEA metadata builder.

So HCA-SCEA Metadata Converter:

HCA bundle JSON -> Common Data Model
Common Data Model -> SCEA metadata

#### Work in progress
- automatically update the config based on metadata versions attached to data in DSS
- automatic data discovery with ability to configure exclusions based on Atlas's dataset requirements e.g. no imaging, drop seq etc. inc assert experiment type aka 'RNA-seq of coding RNA from single cells' assumption.
