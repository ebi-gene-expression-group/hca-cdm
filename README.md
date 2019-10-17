# HCA to Atlas Metadata Converter

HCA metadata into Atlas Common Model python objects. Used by https://github.com/ebi-gene-expression-group/common-datamodel to write and validate MAGE-TAB.

## Quick start

```
pip3 install git+https://github.com/ebi-gene-expression-group/common-datamodel
pip3 install git+https://github.com/ebi-gene-expression-group/hca-cdm
```


From example.py:
```
from hcacdm.ProjectImporter import convert

translation_config_file = 'https://raw.githubusercontent.com/ebi-gene-expression-group/common-datamodel/master/datamodel/config/datamodel_mapping_config.json'
hca_project_uuid = 'cc95ff89-2e68-4a08-a234-480eca21ce79'
submission_object = convert(hca_project_uuid, translation_config_file)
```

#### Work in progress
- automatically update the config based on metadata versions attached to data in DSS
- automatic data discovery with ability to configure exclusions based on Atlas's dataset requirements e.g. no imaging, drop seq etc. inc assert experiment type aka 'RNA-seq of coding RNA from single cells' assumption.

## Editing the config


The config is part of the common data model repo found here 'https://raw.githubusercontent.com/ebi-gene-expression-group/common-datamodel/master/datamodel/config/datamodel_mapping_config.json'

Config allows:
- mapping attributes
- mapping entities
- succinct special handling functions
- handling nested entities
- adding links via entity alias

Each Atlas attribute is nested under it's respective entity type. Each attribute has entiries for their respective mapping source. Currently ae (aka Array Express) and hca (aka Human Cell Atlas DCP) are currently supported. Under the 'hca' label each attribute has several elements described below.

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

