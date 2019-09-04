'''

highest_biological_entity_get
follow path for highest biological entity


Project/Study Methods (one only)

import_string
import_publication
import_contact
get_protocolrefs
get_projectrefs
get_experiment_type = 'RNA-seq of coding RNA from single cells'
get_study_type = 'singlecell'


Sample Methods (one per assay)

All these sample features need to be extracted from the lowest entity in the HCA entity chain

get_sample_name = a join of biomaterials ids?
get_sample_accession
get_sample_taxon pull
get_sample_taxon_id
get_sample_material_type = donor, specimen, cell suspension etc
get_sample_description
get_extra_sample_attributes get everything else up the chain and put it in an array here.

Protocol Methods (multiple but no repeats)

get_extra_protocol_parameters (get everything else in the protocol into this array THIS IS NOT IN THE SCHEMA YET)
get_protocol_type ["protocol", "type", "ontology"] or text whichever exists
get_protocol_hardware return machine name only if on sequencing protocol. Maybe kit name for others


Assay Methods (one per assay)

get_hca_bundle_uuid
get_technology_type = 'sequencing assay'
get_protocol_refs (List fo all the protocol unique names for linking)
get_sample_refs link to one sample per assay (this has condensed info about higher level samples)

Assay Data Methods

get_assay_process_uuid (thinking the process ID is useful here but it might looks really strange in the MAGE-TAB but its good for ref purposes
import_files (similar to the publicationa nd contact import function)
get_assay_ref (get alias from assay entity)

Publication Methods

Contacts Methods

get_first_name from ["project", "contributors", "name"]
get_last_name
get_middle_initial

Datafile Methods

get_checksum_method = 'MD5'

lib_attribs Methods

get_library_selection = 'cDNA'
get_library_strategy = 'RNA-Seq'
get_library_source = 'transcriptomic single cell'


NB no microarray entity, no sequencing_assay entity, no analysis entity

'''

__author__ = "hewgreen"
__license__ = "Apache 2.0"
__date__ = "30/08/2019"

import sys
from collections import OrderedDict

class fetch_metadata:
    '''
    func should return 1 dict for 1 common datamodel entity which can then be added to the project translated output
    by the main script

    No logic needed to catch duplicates, this is in the main script.

    alias should be dict key and value should be dict of attributes e.g. {"sample":{"alias_of_sample":{ATTRIBUTES GO HERE}}}

    special handling functions are built into the class
    '''

    def __init__(self, bundle, common_entity_type, attribute_translation, bundle_info, metadata_files, metadata_files_by_uuid, granularity):

        print('WORKING ON ENITY TYPE: {}'.format(common_entity_type))

        self.bundle = bundle
        self.common_entity_type = common_entity_type
        self.attribute_translation = attribute_translation
        self.bundle_info = bundle_info
        self.metadata_files = metadata_files
        self.metadata_files_by_uuid = metadata_files_by_uuid
        self.granularity = granularity

        attribute_value_dict = {}
        for common_attribute, t in attribute_translation.items():
            print('WORKING ON ATTRIBUTE: {}'.format(common_attribute))

            # read config file for each attribute
            self.import_parent = t.get('import').get('hca').get('parent')  # HCA ENTITY name e.g. project_json
            self.import_path = t.get('import').get('hca').get(
                'path')  # hca listed path to attribute (need updating as schema evolves)
            self.special_import_method = t.get('import').get('hca').get(
                'method')  # used by converter to do bespoke translations
            self.import_translation = t.get('import').get('hca').get('translation', None)  # used to do value translation

            # check required fields
            # assert getattr(fetch_metadata, self.special_import_method), 'CONFIG ERROR: Missing special function {} from config for attribute {}'.format(self.special_import_method, common_attribute)

            # get attribute value
            if self.special_import_method:  # special_import_method trumps other general import rule
                attribute_value = getattr(fetch_metadata, self.special_import_method)(self)
                attribute_value_dict[common_attribute] = attribute_value

            you are here. rewrite this geven that there aren't many general ones. this needs to skip entities that have already been read. This may make a bit of work to solve. Simple is best!!!



            # general import
            elif self.common_entity_type in (self.granularity['one_entity_per_project'] + self.granularity['one_entity_per_hca_assay']): # take first entity in graph for these entity types
                assert self.import_path, 'CONFIG ERROR: Missing HCA import path in config for attribute: {}.{}'.format(
                    self.common_entity_type, common_attribute)
                attribute_value = self.general_get_value_for_first_entity()

            elif self.common_entity_type in self.granularity['unique_project_wide']:
                print('NOT SUPPORTED YET') # todo
                sys.exit()
            else:
                raise Exception('CONFIG ERROR: Entity type {} not recognosed'.format(common_entity_type))

            # set entity name
            if common_attribute == 'alias':
                alias = attribute_value

        self.translated_entity_metadata = {common_entity_type:{alias:attribute_value_dict}}


    # Assay Methods
    def get_hca_bundle_uuid(self):
        return self.bundle.get('metadata').get('uuid')

    # Sample Methods

    def highest_biological_entity_get(self):
        # for use when import parent is unknown but general type is biomaterial
        highest_biomaterial = self.bundle_info.ordered_biomaterials[0]
        d = self.metadata_files_by_uuid.get(highest_biomaterial)
        return self.recursive_get(d)

    def get_sample_material_type(self):
        highest_biomaterial_uuid = self.bundle_info.ordered_biomaterials[0]
        highest_biomaterial = self.metadata_files_by_uuid.get(highest_biomaterial_uuid)
        return highest_biomaterial.get('describedBy').split('/')[-1]

    def get_other_biomaterial_attributes(self):
        '''
        Extract extra attributes not captured by common data model schema.
        Look at biomaterials in order of sequence in the graph.
        todo ignore fields that have already been added to the model for the top entity.
        todo this counter may need some refactoring when the design is complete. This is to protect against replacing keys of the same attribute in the column headers of the sdrf. This needs testing with real data.
        '''

        def list_handler(in_list):
            condensed_value = []
            for entry in in_list:
                if isinstance(entry, dict) and 'ontology' in entry:
                    ontology = entry.get('ontology', None)
                    condensed_value.append(ontology)
                elif isinstance(entry, (int, str)):
                    condensed_value.append(entry)
                else:
                    raise Exception('Data type not yet supported at this level. Update the parser to include {}'.format(
                        type(entry)))
            return condensed_value

        extra_attributes = OrderedDict()


        entity_counter = {}

        for biomaterial_uuid in self.bundle_info.ordered_biomaterials:
            # # certain branches can be ignored when exploring the tree.

            biomaterial_metadata = self.metadata_files_by_uuid.get(biomaterial_uuid)
            material_type = biomaterial_metadata.get('describedBy').split('/')[-1]

            # counter
            if material_type in entity_counter:
                entity_counter[material_type] += 1
            else:
                entity_counter[material_type] = 1
            entity_type_count = material_type + '_' + str(entity_counter.get(material_type))


            ignore_top_level = ['schema_type', 'provenance', 'describedBy']
            entity_extra_attributes = {}

            # Explicit metadata parser only supports expected levels of nesting and datatypes at those levels by design but it otherwise it not hard coded.

            for top_level_attribute, top_level_value in biomaterial_metadata.items():
                if top_level_attribute in ignore_top_level:
                    continue
                if isinstance(top_level_value, (dict, list)) == False:
                    entity_extra_attributes[entity_type_count + '.' + top_level_attribute] = top_level_value
                elif isinstance(top_level_value, dict):
                    for mid_level_attribute , mid_level_value in top_level_value.items():
                        if isinstance(mid_level_value, (dict, list)) == False:
                            entity_extra_attributes[entity_type_count + '.' + top_level_attribute + '.' + mid_level_attribute] = mid_level_value
                        elif isinstance(mid_level_value, list):
                            condensed_mid_level_value = list_handler(mid_level_value)
                            entity_extra_attributes[entity_type_count + '.' + top_level_attribute + '.' + mid_level_attribute] = condensed_mid_level_value
                        elif isinstance(mid_level_value, dict):
                            for low_level_attribute, low_level_value in mid_level_value.items():
                                if isinstance(low_level_value, (dict, list)) == False:
                                    entity_extra_attributes[entity_type_count + '.' + top_level_attribute + '.' + mid_level_attribute + '.' + low_level_attribute] = low_level_value
                                else:
                                    raise Exception('4th level nesting detected but not expected. See {}'.format(top_level_attribute + '.' + mid_level_attribute + '.' + low_level_attribute))
                        else:
                            raise Exception('Value type {} not supported'.format(type(mid_level_value)))
                else:
                    assert isinstance(top_level_value, list)
                    entity_extra_attributes[entity_type_count + '.' + top_level_attribute] = list_handler(top_level_value)

                extra_attributes.update(entity_extra_attributes)

        return extra_attributes


    def recursive_get(self, d):
        for key in self.import_path:
            if isinstance(d, list):
                d = [x.get(key, None) for x in d]
            else:
                d = d.get(key, None)
        return d









# todo maybe add one formatter func to remove [] and underscores?



    # def general_get_value_for_first_entity(self):
    #     # NB ONLY GETS VALUE FOR FIRST ENTITY IN GRAPH!
    #     # Does not work for protocols
    #     assert self.common_entity_type not in self.granularity['unique_project_wide'], 'Inappropriate usage of method'
    #     assert self.import_parent != None, 'Method required an import parent entity'
    #     assert self.import_path != None, 'Method required an import path'
    #
    #     hca_entity_type = self.import_parent
    #     metadata_files_of_type = self.metadata_files.get(hca_entity_type)
    #     uuid = metadata_files_of_type[0].get('provenance').get('document_id')
    #     if len(metadata_files_of_type) == 1:
    #         d = metadata_files_of_type[0]
    #     elif len(metadata_files_of_type) >= 1 and uuid in self.bundle_info.ordered_biomaterials:
    #         d = self.metadata_files_by_uuid.get(self.bundle_info.top_nodes[0])
    #     else:
    #         raise Exception('This method can only be used for biomaterial attributes and single use entities')
    #     return self.recursive_get(d)





