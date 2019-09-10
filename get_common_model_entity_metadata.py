__author__ = "hewgreen"
__license__ = "Apache 2.0"
__date__ = "30/08/2019"

from collections import OrderedDict
# todo maybe add one formatter func to remove [] and underscores?

class fetch_entity_metadata_translation:
    '''
    - func should return 1 dict for 1 common datamodel entity which can then be added to the project translated output
    by the main script
    - No logic needed to catch duplicates, this is in the main script.
    - alias should be dict key and value should be dict of attributes e.g. {"sample":{"alias_of_sample":{ATTRIBUTES GO HERE}}}
    - special handling functions are built into the class
    '''

    def __init__(self, translation_params):


        #initialize
        self.bundle = translation_params.get('bundle')
        self.common_entity_type = translation_params.get('common_entity_type')
        self.attribute_translation = translation_params.get('attribute_translation')
        self.bundle_graph = translation_params.get('bundle_graph')
        self.metadata_files = translation_params.get('metadata_files')
        self.metadata_files_by_uuid = translation_params.get('metadata_files_by_uuid')
        self.translation_config = translation_params.get('translation_config')
        self.bundle_uuid = self.bundle.get('metadata').get('uuid')
        print('WORKING ON ENITY TYPE: {}'.format(self.common_entity_type))

        attribute_value_dict = {}
        for common_attribute, t in self.attribute_translation.items():
            self.common_attribute = common_attribute
            self.t = t

            print('WORKING ON ATTRIBUTE: {}'.format(self.common_attribute))

            attribute_value = self.get_attribute_value()
            attribute_value_dict[self.common_attribute] = attribute_value
            print('{} : {}'.format(self.common_attribute, attribute_value))

        self.translated_entity_metadata = {self.common_entity_type:{attribute_value_dict.get('alias'):attribute_value_dict}} # alias is required

    # main get method
    def get_attribute_value(self):
        if self.t.get('import', None).get('hca', None) == None: # if 'hca' is missing in config, return None (e.g. hca doesn't have fax attribute)
            return

        # CONFIG REQUIRED hca listed path to attribute (need updating as schema evolves) HCA ENTITY name e.g. project_json is top of list
        self.import_path = self.t.get('import').get('hca').get('path')
        assert self.import_path or self.import_path == [], 'Missing import_path in config for attribute {}'.format(
            self.common_entity_type + '.' + self.common_attribute)
        # CONFIG REQUIRED used by converter to do all translations
        self.import_method = self.t.get('import').get('hca').get('method')
        assert self.import_method, 'Missing special_import_method in config for attribute {}'.format(
            self.common_entity_type + '.' + self.common_attribute)
        # CONFIG OPTIONAL used to do value translation
        self.import_translation = self.t.get('import').get('hca').get('translation', None)

        # get attribute value
        attribute_value = getattr(fetch_entity_metadata_translation, self.import_method)(self)
        return attribute_value


    # General Methods
    def import_string(self):
        # follow path and return value
        files = self.metadata_files.get(self.import_path[0])
        assert len(files) == 1, 'This method expects 1 file per bundle. Detected mutiple {} entities in bundle {}'.format(self.common_entity_type, self.bundle_uuid)
        value = files[0]
        for level in self.import_path[1:]:
            value = value.get(level)
        return value

    def import_string_from_selected_entity(self):
        value = self.selected_entity
        for level in self.import_path:
            value = value.get(level, None)
            if value == None:
                return None
        return value

    def import_nested(self):
        # imports nested entities (contacts, publications etc)
        entities = self.import_string()
        if entities == None:
            return None
        nested_attributes_as_list = []
        for selected_entity in entities:
            self.selected_entity = selected_entity
            entity_metadata = {}
            for common_attribute, t in self.translation_config.get(self.nested_entity_type).items():
                if not t.get('import').get('hca', False): # skip fields without a hca entry
                    continue
                self.common_attribute = common_attribute
                self.t = t

                # print('WORKING ON NESTED {}'.format(common_attribute))
                attribute_value = self.get_attribute_value()
                entity_metadata[common_attribute] = attribute_value
                # print('NESTED {} : {}'.format(common_attribute, attribute_value))
            nested_attributes_as_list.append(entity_metadata)
        self.common_attribute = self.nested_entity_type
        return nested_attributes_as_list # todo maybe need to wrap up as dict with alias rather than use a list

    def placeholder(self):
        '''
        Some entries (e.g. protocolrefs "protocol accessions/name used in the study")
        require a whole project search which can only be performed at the end of the loop.
        This method adds a placeholder marking the method for update after the fact.
        todo add ability to build after the fact
        '''
        return str(self.common_attribute) + '_PLACEHOLDER'

    # Project Methods
    def import_nested_publications(self):
        self.nested_entity_type = 'publication'
        return self.import_nested()
        # todo check when I have an example bundle with a publication
        # todo add method for status

    def import_nested_contacts(self):
        self.nested_entity_type = 'contact'
        return self.import_nested()

    # Contacts Methods

    def import_first_name(self):
        name = self.import_string_from_selected_entity()
        try:
            return name.split(',')[0]
        except IndexError:
            return None

    def import_last_name(self):
        name = self.import_string_from_selected_entity()
        try:
            return name.split(',')[-1]
        except IndexError:
            return None

    def get_middle_initial(self):
        name = self.import_string_from_selected_entity()
        try:
            return name.split(',')[1][0]
        except IndexError:
            return None

    # Study Methods

    #todo BUG 'null' is the name of the study entity. Alias shoudl be required field or have a unique backup.

    def get_experiment_type(self):
        return 'RNA-seq of coding RNA from single cells'

    def get_study_type(self):
        return 'singlecell'

    # Assay Methods
    def get_hca_bundle_uuid(self):
        return self.bundle_uuid

    # Sample Methods

    #todo BUG sample alias is 'sample'. Needs looking at.
    #todo BUG only detecting 1 donor, 1 specimen and 1 cell suspension for the dataset.

    def highest_biological_entity_get(self):
        # for use when import parent is unknown but general type is biomaterial
        highest_biomaterial = self.bundle_graph.ordered_biomaterials[0]
        d = self.metadata_files_by_uuid.get(highest_biomaterial)
        for key in self.import_path:
            if isinstance(d, list):
                d = [x.get(key, None) for x in d]
            else:
                d = d.get(key, None)
        return d

    def get_sample_material_type(self):
        highest_biomaterial_uuid = self.bundle_graph.ordered_biomaterials[0]
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

        for biomaterial_uuid in self.bundle_graph.ordered_biomaterials:
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

    # Data File Methods
