__author__ = "hewgreen"
__license__ = "Apache 2.0"
__date__ = "30/08/2019"

from collections import OrderedDict
import sys
# todo maybe add one formatter func to remove [] and underscores?

class fetch_entity_metadata_translation:
    '''
    - class should return 1 dict for 1 common datamodel entity which can then be added to the project translated output
    by the main script. This may include a list of nested entities within the top entity.
    - No logic needed to catch duplicates, this is in the main script.
    - alias should be dict key and value should be dict of attributes e.g. {"sample":{"alias_of_sample":{ATTRIBUTES GO HERE}}}
    - special handling functions are built into the class (see bottom)
    '''

    def __init__(self, translation_params, protocol_uuid=None):


        #initialize
        self.bundle = translation_params.get('bundle')
        self.common_entity_type = translation_params.get('common_entity_type')
        self.attribute_translation = translation_params.get('attribute_translation')
        self.bundle_graph = translation_params.get('bundle_graph')
        self.metadata_files = translation_params.get('metadata_files')
        self.metadata_files_by_uuid = translation_params.get('metadata_files_by_uuid')
        self.translation_config = translation_params.get('translation_config')
        self.bundle_uuid = self.bundle.get('metadata').get('uuid')
        self.protocol_uuid = protocol_uuid
        # print('WORKING ON ENITY TYPE: {}'.format(self.common_entity_type))

        self.links = {self.common_entity_type : []}

        attribute_value_dict = {}
        for common_attribute, t in self.attribute_translation.items():
            self.common_attribute = common_attribute
            self.t = t

            # print('WORKING ON ATTRIBUTE: {}'.format(self.common_attribute))

            attribute_value = self.get_attribute_value()
            attribute_value_dict[self.common_attribute] = attribute_value
            # print('{} : {}'.format(self.common_attribute, attribute_value))

            if common_attribute == 'alias':
                self.links[self.common_entity_type].append(attribute_value)

        self.translated_entity_metadata = attribute_value_dict # alias is required

    # main get method
    def get_attribute_value(self):
        if self.t.get('import', None).get('hca', None) == None: # if 'hca' is missing in config, return None (e.g. hca doesn't have fax attribute)
            return

        # CONFIG REQUIRED hca listed path to attribute (need updating as schema evolves) HCA ENTITY name e.g. project_json is top of list
        self.import_path = self.t.get('import').get('hca').get('path')
        self.import_translation = self.t.get('import').get('hca').get('translation', None)
        if isinstance(self.import_translation, dict):
            self.import_translation.update({a: None for a, b in self.import_translation.items() if b == 'null'}) # JSON doesn't support None
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
        assert len(self.import_path) > 0, 'Path is required to use this method. Please add one to the config for this attribute. See {}'.format(str(self.common_entity_type+ '.' + self.common_attribute))
        files = self.metadata_files.get(self.import_path[0])
        if not files:
            # todo log things that can't be found here as a warning rather than blocking the conversion
            return None
        # assert files, 'File {} not found in bundle {}'.format(self.import_path[0], self.bundle_uuid)
        assert len(files) == 1, 'This method expects 1 file per bundle. Detected mutiple {} entities in bundle {}'.format(self.common_entity_type, self.bundle_uuid)
        value = files[0]

        for level in self.import_path[1:]:
            if value:
                value = value.get(level, None)
            else:
                continue

        # ontology_label is preferred but text field should be serched if value is not available
        if self.import_path[-1] == 'ontology_label' and value == None:
            self.import_path[-1] = 'text'
            self.import_string()

        return value

    def import_string_from_selected_entity(self):
        value = self.selected_entity
        for level in self.import_path:
            value = value.get(level, None)
            if value == None:
                return None
        return value

    def import_nested(self, entities):
        # imports nested entities (contacts, publications etc)
        if entities == None:
            return None
        nested_attributes_as_list = []
        for selected_entity in entities:
            self.selected_entity = selected_entity
            entity_metadata = {}
            nested_entity_translator_config = self.translation_config.get(self.nested_entity_type)
            assert nested_entity_translator_config, 'Config missing nested entity describing {}'.format(self.nested_entity_type)
            for common_attribute, t in nested_entity_translator_config.items():
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
        return nested_attributes_as_list

    def use_translation(self):
        # this method relies ont he translation field in the config to perform value manipulation.
        assert self.import_translation != None, 'This method requires a translation string or dict in the config file.'
        assert isinstance(self.import_translation, (str, dict)), 'Translation in config should be of type str or dict'
        if type(self.import_translation) == str:
            return self.import_translation
        elif type(self.import_translation) == dict:
            value = str(self.import_string())
            value_translation = self.import_translation.get(value, 'NOT FOUND')

            # Stop if mapping not found. Expects all values be in translation dict even if unchanged. The behaivor may need to change in the future.
            assert value_translation != 'NOT FOUND', 'Value "{}" is not in the config dict and cannot be converted. See {}'.format(value, str(self.common_entity_type+ '.' + self.common_attribute))
            return value_translation

    # Project Methods
    def import_nested_publications(self):
        self.nested_entity_type = 'publication'
        entities = self.import_string()
        return self.import_nested(entities)

    def import_nested_contacts(self):
        self.nested_entity_type = 'contact'
        entities = self.import_string()
        return self.import_nested(entities)

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

    # Assay Methods
    def get_hca_bundle_uuid(self):
        return self.bundle_uuid

    # Sample Methods

    def lowest_biological_entity_get(self):
        # for use when import parent is unknown but general type is biomaterial
        lowest_biomaterial = self.bundle_graph.ordered_biomaterials[-1]
        d = self.metadata_files_by_uuid.get(lowest_biomaterial)
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
        todo ignore fields that have already been added to the model for the last entity.
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

    # Assay Data Methods

    def import_nested_data_files(self):
        # data files are 'nested' in the common model but not in the HCA model so entities are derived differently
        self.nested_entity_type = 'data_file'
        entities = self.metadata_files
        for level in self.import_path:
            if entities:
                entities = entities.get(level, None)
            else:
                continue
        return self.import_nested(entities)

    # Data file method

    def get_checksum_method(self):
        # only return method if the file has a checksum
        checksum_method_path = self.import_path
        self.import_path = self.translation_config\
                            .get('data_file', None)\
                            .get('checksum', None)\
                            .get('import', None)\
                            .get('hca', None)\
                            .get('path', None)
        assert self.import_path, 'Hardcoded path in code has broken. Please repair.'
        if self.import_string_from_selected_entity():
            self.import_path = checksum_method_path
            return self.import_translation
        else:
            return None

    # Protocol Methods

    def import_string_from_protocol(self):
        # required to ensure strings are collected from selected protocol
        self.selected_entity = self.metadata_files_by_uuid.get(self.protocol_uuid)
        self.import_path = self.import_path
        return self.import_string_from_selected_entity()

    def get_protocol_type(self):
        return self.import_string_from_protocol().split('/')[-1]

    def get_protocol_operator(self):
        # traverse graph from protocol to find metadata on process nodes
        protocol_in_edges = list(self.bundle_graph.G.in_edges([self.protocol_uuid]))
        process_in_nodes = set()
        for edge in protocol_in_edges:
            assert len(edge) == 2, 'Strange edge detected len != 2'
            for node in edge:
                if node != self.protocol_uuid:
                    process_in_nodes.add(node)

        operators = []
        for process in process_in_nodes:
            self.selected_entity = self.metadata_files_by_uuid.get(process)
            operator = self.import_string_from_selected_entity()
            if operator:
                operators.append(self.import_string_from_selected_entity())
        if len(operators) > 1:
            return ', '.join(operators)
        elif len(operators) == 0:
            return None
        else:
            assert len(operators) == 1, 'Function logic is failing'
            return operators[0]

    # Entity Linking Methods

    def placeholder(self):
        # back filled in with linking fields
        return str(self.common_attribute) + '_PLACEHOLDER'