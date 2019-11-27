'''
This scrip converts HCA json entities into cdm mapped json entities.
It works on one entity at a time.
Conversion methods described in the converter config file should live in this script.
'''

__author__ = "hewgreen"
__license__ = "Apache 2.0"
__date__ = "30/08/2019"

from collections import OrderedDict
import sys
from hcacdm import aux_func as aux_func

logger = aux_func.get_logger(__name__)


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
        self.bundle_fqid = self.bundle.get('bundle_fqid')
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
            # print('{} : {}'.format(self.self.common_attribute, attribute_value))

            if self.common_attribute == 'alias':
                self.links[self.common_entity_type].append(attribute_value)

        # strip keys with None values, added back when obj are created
        stripped_attribute_value_dict = {}
        for key, value in attribute_value_dict.items():
            if value == None:
                continue
            elif isinstance(value, list):
                if not value: # empty list strip
                    continue
                elif all(isinstance(x, dict) for x in value):
                    new_value = []
                    for obj in value:
                        new_value.append({k: v for k, v in obj.items() if v != None}) # nested None strip
                    stripped_attribute_value_dict[key] = new_value
                else:
                    stripped_attribute_value_dict[key] = value
            else:
                stripped_attribute_value_dict[key] = value

        self.translated_entity_metadata = stripped_attribute_value_dict # return final entity metadata

    # main get method
    def get_attribute_value(self):


        if self.t.get('import', None) == None:
            return
        elif self.t.get('import', None).get('hca', None) == None: # if 'hca' is missing in config, return None (e.g. hca doesn't have fax attribute)
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

        # ensure correct object type map attribute value type to requested type
        return self.object_type_mapping(attribute_value)

    def object_type_mapping(self, attribute_value):
        # logic and rules for mapping cdm to HCA value types
        # this only returns str, list or dict. Final export step converts to cdm python objects as dicated in config.

        # pull type information from config
        error_raise = False
        cdm_required_type = self.t.get('type')
        allowed_cdm_required_types = ['string', 'array', 'attribute', 'integer','object']  # allowed values in config types
        allowed_cdm_required_array_types = ['publication', 'contact', 'data_file','string', 'attribute']  # allowed values in config item

        assert cdm_required_type in allowed_cdm_required_types, 'Unrecognised type "{}" in config. See entity {}.{}'.format(cdm_required_type, self.common_entity_type, self.common_attribute)  # assertions just warn about new types
        if cdm_required_type == 'array':
            assert self.t.get('items'), 'Config with array type requires "item" entry. See {}.{}'.format(
                self.common_entity_type, self.common_attribute)
            array_item_type = self.t.get('items')
            if array_item_type not in allowed_cdm_required_array_types:
                raise AttributeError('Unrecognised item {} in config. See entity {}.{}'.format(array_item_type, self.common_entity_type, self.common_attribute))  # assertions just warn about new type
        else:
            array_item_type = None

        # mapping rules - NB more may be needed as config evolves

        if cdm_required_type in ['string', 'integer']:
            if attribute_value == None:
                return None
            if isinstance(attribute_value, (str, int)):
                return str(attribute_value) # int not allowed, force str
            if isinstance(attribute_value, dict) and all(y in attribute_value for y in ['ontology', 'ontology_label', 'text']): # single ontology dict
                ontology = attribute_value.get('ontology')
                ontology_label = attribute_value.get('ontology_label')
                text = attribute_value.get('text')
                if ontology_label: # label preferred
                    return ontology_label
                elif ontology:
                    return ontology
                elif text:
                    return text
                else:
                    return None
            elif isinstance(attribute_value, list):
                if len(attribute_value) == 0:
                    return None
                elif len(attribute_value) == 1:
                    if cdm_required_type == 'string':
                        return str(attribute_value[0])
                    elif cdm_required_type == 'string':
                        return int(attribute_value[0])
                    else:
                        error_raise = True
                else:
                    error_raise = True
            else:
                error_raise = True


        elif cdm_required_type == 'array':
            if attribute_value == None:
                return None
            elif array_item_type == 'string':
                if isinstance(attribute_value, list) and all(isinstance(y, str) for y in attribute_value):
                    return attribute_value
                elif isinstance(attribute_value, str):
                    return [attribute_value]
            elif array_item_type in ['publication', 'contact', 'data_file']:
                if isinstance(attribute_value, (list)):
                    return (attribute_value)
                else:
                    error_raise = True
            elif array_item_type == 'attribute':
                if isinstance(attribute_value, (str)):
                    return [{'value': attribute_value}]
                else:
                    error_raise = True
            else:
                error_raise = True


        elif cdm_required_type in ['object', 'attribute']:
            if attribute_value == None:
                return None
            elif isinstance(attribute_value, (str)):
                if attribute_value.endswith('_PLACEHOLDER'):
                    return attribute_value
                else:
                    return {'value': attribute_value}
            elif isinstance(attribute_value, (dict)):
                return (attribute_value)
            else:
                error_raise = True

        else:
            error_raise = True

        # todo deal with lists of ontologies

        if error_raise:
            logger.critical('Missing logic for object typing {}.{}'.format(self.common_entity_type, self.common_attribute))
            raise AttributeError('Missing logic for object typing. \n'
                                '{}.{}\n'
                                'Value: {}\n'
                                'HCA metadata contains: {}\n'
                                'Common data model requires type: {}\n'
                                'Common data model array type requires: {}\n'
                                ''.format(self.common_entity_type,
                                          self.common_attribute,
                                attribute_value,
                                type(attribute_value),
                                cdm_required_type,
                                array_item_type))


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

        return value

    def import_string_from_selected_entity(self):
        value = self.selected_entity
        for level in self.import_path:
            value = value.get(level, None)
            if value == None:
                return None
        return value

    def import_nested(self, entities, nested_entity_type, top_level_common_attribute):
        # imports nested entities (contacts, publications etc)
        if entities == None:
            return None
        nested_attributes_as_list = []
        for selected_entity in entities:
            self.selected_entity = selected_entity
            entity_metadata = {}
            nested_entity_translator_config = self.translation_config.get(nested_entity_type)
            assert nested_entity_translator_config, 'Config missing nested entity describing {}'.format(self.nested_entity_type)
            for common_attribute, t in nested_entity_translator_config.items():
                if not t.get('import').get('hca', False): # skip fields without a hca entry
                    continue
                self.common_attribute = common_attribute
                self.t = t

                # print('WORKING ON NESTED {}'.format(self.common_attribute))
                attribute_value = self.get_attribute_value()
                entity_metadata[self.common_attribute] = attribute_value
                self.common_attribute = str(top_level_common_attribute) # reset
                self.t = self.attribute_translation.get(top_level_common_attribute)  # reset
                # print('NESTED {} : {}'.format(common_attribute, attribute_value))
            nested_attributes_as_list.append(entity_metadata)
        return nested_attributes_as_list

    def use_translation(self):
        # this method relies on the translation field in the config to perform value manipulation.
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
        top_level_common_attribute = str(self.common_attribute)
        nested_entity_type = 'publication'
        entities = self.import_string()
        return self.import_nested(entities, nested_entity_type, top_level_common_attribute)

    def import_nested_contacts(self):
        top_level_common_attribute = str(self.common_attribute)
        nested_entity_type = 'contact'
        entities = self.import_string()
        return self.import_nested(entities, nested_entity_type, top_level_common_attribute)

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

    def get_hca_bundle_version(self):
        return self.bundle_fqid.replace(self.bundle_uuid + '.', '')

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
        todo ignore fields that have already been added to the model for the last entity. Not just a blanket skip first entity!!
        todo this counter may need some refactoring when the design is complete. This is to protect against replacing keys of the same attribute in the column headers of the sdrf. This needs testing with real data.
        todo make dict to suite Attribute class.
        '''

        def list_handler(in_list):
            condensed_value = []
            for entry in in_list:
                if isinstance(entry, dict) and any(a in entry for a in ['ontology', 'text', 'ontology_label']):
                    value = entry.get('ontology', None)
                    if not value:
                        value = entry.get('text', None)
                    condensed_value.append(value)
                elif isinstance(entry, (int, str)):
                    condensed_value.append(entry)
                else:
                    logger.warn('Data type not yet supported at this level. Update the parser to include {}'.format(type(entry)))
                    condensed_value = None

            return condensed_value

        extra_attributes = OrderedDict()


        entity_counter = {}

        for biomaterial_uuid in self.bundle_graph.ordered_biomaterials:
            # certain branches can be ignored when exploring the tree.
            ignore_top_level = ['schema_type', 'provenance', 'describedBy']

            biomaterial_metadata = self.metadata_files_by_uuid.get(biomaterial_uuid)
            material_type = biomaterial_metadata.get('describedBy').split('/')[-1]

            # counter for multi entity chains e.g. cell sus -> cell sus
            if material_type in entity_counter:
                entity_counter[material_type] += 1
            else:
                entity_counter[material_type] = 1
            entity_type_count = material_type + '_' + str(entity_counter.get(material_type))


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
                                    logger.critical('4th level nesting detected but not expected. See {}'.format(top_level_attribute + '.' + mid_level_attribute + '.' + low_level_attribute))
                                    raise Exception('4th level nesting detected but not expected. See {}'.format(top_level_attribute + '.' + mid_level_attribute + '.' + low_level_attribute))
                        else:
                            logger.critical('Value type {} not supported'.format(type(mid_level_value)))
                            raise Exception('Value type {} not supported'.format(type(mid_level_value)))
                else:
                    assert isinstance(top_level_value, list)
                    entity_extra_attributes[entity_type_count + '.' + top_level_attribute] = list_handler(top_level_value)

                extra_attributes.update(entity_extra_attributes)

        # condense ontologies and units

        '''
        This relies on the HCA style guide. Assumptions:
        - ending ontology attributes with '.text', '.ontology' and '.ontology_label'
        - ending unit with '_unit.text', '_unit.ontology' and '_unit.ontology_label'
        '''

        condensed_extra_attributes = OrderedDict()
        unit_attributes = {}
        ontology_attributes = {}
        ontology_endings = {
            '.ontology_label' : 'value',
            '.ontology': 'term_accession'
        }
        unit_endings = {
            '_unit.text': 'value',
            '_unit.ontology_label': 'unit_type',
            '_unit.ontology': 'term_accession',
        }
        # term_source should always be HCAO
        # unit needs adding if any unit info is found


        # look for nested entries and split
        for sub_attribute_name, sub_attribute_value in extra_attributes.items():
            # name_ending = sub_attribute_name.split('.')[-1]
            if any(sub_attribute_name.endswith(y) for y in unit_endings):
                unit_attributes[sub_attribute_name] = sub_attribute_value
            elif any(sub_attribute_name.endswith(y) for y in ontology_endings):
                ontology_attributes[sub_attribute_name] = sub_attribute_value
            else:
                condensed_extra_attributes[sub_attribute_name] = {'value' : sub_attribute_value}

        def sub_attribute_lookup(subattribute_list, endings_dict, mode):
            for sub_attribute_name, sub_attribute_value in subattribute_list.items():
                ending = [k for k, v in endings_dict.items() if sub_attribute_name.endswith(k)][0]
                info_key = endings_dict.get(ending)
                stripped_name = sub_attribute_name.replace(ending, '')

                if condensed_extra_attributes.get(stripped_name, None):
                    condensed_attribute = stripped_name
                elif condensed_extra_attributes.get(stripped_name + '.text', None):
                    condensed_attribute = stripped_name + '.text'
                else:
                    logger.critical('Cannot find attribute called {} to add {} to.'.format(stripped_name, sub_attribute_name))
                    raise ValueError('Cannot find attribute called {} to add {} to.'.format(stripped_name, sub_attribute_name))

                if info_key == 'term_accession' and mode == 'ontology':
                    condensed_extra_attributes[condensed_attribute]['term_source'] = 'http://ontology.staging.data.humancellatlas.org/index'
                    condensed_extra_attributes[condensed_attribute][info_key] = sub_attribute_value
                elif info_key == 'value' and mode == 'ontology':
                    continue # this information is lost but can be regained using the curie
                elif info_key == 'term_accession' and mode == 'unit':
                    if condensed_extra_attributes[condensed_attribute].get('unit'):
                        condensed_extra_attributes[condensed_attribute]['unit']['term_accession'] = sub_attribute_value
                    else:
                        condensed_extra_attributes[condensed_attribute]['unit'] = {'term_accession':sub_attribute_value}
                elif info_key == 'value' and mode == 'unit':
                    if condensed_extra_attributes[condensed_attribute].get('unit'):
                        condensed_extra_attributes[condensed_attribute]['unit']['value'] = sub_attribute_value
                    else:
                        condensed_extra_attributes[condensed_attribute]['unit'] = {'value':sub_attribute_value}
                elif info_key == 'unit_type' and mode == 'unit':
                    if condensed_extra_attributes[condensed_attribute].get('unit'):
                        condensed_extra_attributes[condensed_attribute]['unit']['unit_type'] = sub_attribute_value
                    else:
                        condensed_extra_attributes[condensed_attribute]['unit'] = {'unit_type':sub_attribute_value}
                else:
                    logger.critical('Cannot place value {} because {} is not defined in code.'.format(sub_attribute_value, info_key))
                    raise ValueError('Cannot place value {} because {} is not defined in code.'.format(sub_attribute_value, info_key))

        sub_attribute_lookup(ontology_attributes, ontology_endings, 'ontology')
        sub_attribute_lookup(unit_attributes, unit_endings, 'unit')

        return condensed_extra_attributes

    # Assay Data Methods

    def import_nested_data_files(self):
        # data files are 'nested' in the common model but not in the HCA model so entities are derived differently
        top_level_common_attribute = str(self.common_attribute)
        nested_entity_type = 'data_file'
        entities = self.metadata_files
        for level in self.import_path:
            if entities:
                entities = entities.get(level, None)
            else:
                continue
        return self.import_nested(entities, nested_entity_type, top_level_common_attribute)

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
        hca_protocol_type = self.import_string_from_protocol().split('/')[-1]
        value_translation = self.import_translation.get(hca_protocol_type, 'NOT FOUND')
        # Stop if mapping not found. Expects all values be in translation dict even if unchanged. The behaivor may need to change in the future.
        assert value_translation != 'NOT FOUND', 'Value "{}" is not in the config dict and cannot be converted. See {}'.format(
            hca_protocol_type, str(self.common_entity_type + '.' + self.common_attribute))
        return value_translation

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