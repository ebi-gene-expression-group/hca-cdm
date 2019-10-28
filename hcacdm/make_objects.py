'''
Pass objects as dict. Keys should correspond to those described in 'datamodel'
No objects should be created before this step for clarity.
'''

__author__ = "hewgreen"
__license__ = "Apache 2.0"
__date__ = "08/10/2019"

from hcacdm import ProjectImporter
from datamodel.submission import Submission
from datamodel.sample import Sample
from datamodel.protocol import Protocol
from datamodel.study import Study
from datamodel.project import Project
from datamodel.assay import SingleCellAssay
from datamodel.components import Attribute, Unit
from datamodel.data import AssayData
import re
from collections import defaultdict


class json_to_objects:

    def __init__(self, project_translated_output, translation_config, conversion_info):
        self.conversion_info = conversion_info
        self.project_translated_output = project_translated_output
        self.translation_config = translation_config
        self.object_mapping = {'project': Project,
                               'study': Study,
                               'protocol':Protocol,
                               'sample': Sample,
                               'singlecell_assay': SingleCellAssay,
                               'assay_data': AssayData
                      }
        self.json_python_type_mappings = {
            'array': ['list', 'NoneType'],
            'string': ['str', 'NoneType'],
            'object':['dict', 'collections.OrderedDict', 'NoneType'],
            'None': ['NoneType'],
            'contact': ['dict'],
            'publication': ['dict'],
            'attribute': ['dict'],
            'data_file': ['dict']
        }

        self.main()

    def validate_json_type_vs_config(self, entity, common_entity_type):
        # conversion is done to JSON this func ensures that the JSON type is correct compared to the required type stated in the config

        for attribute_name, attribute_value in entity.items():

            # init
            translation = self.translation_config.get(common_entity_type).get(attribute_name)
            cdm_required_type = translation.get('type')  # expected object type
            cdm_required_nested_type = translation.get('items', 'None')  # expected object type in arrays
            if cdm_required_type not in self.json_python_type_mappings:
                raise ValueError('Unexpected type in config. Missing {}'.format(cdm_required_type))
            elif cdm_required_nested_type not in self.json_python_type_mappings:
                raise ValueError('Unexpected type in config. Missing {}'.format(cdm_required_nested_type))
            python_type = re.search(r"<class '(.*?)'>", (str(type(attribute_value)))).group(1)

            # test return value is valid against config
            if python_type in self.json_python_type_mappings.get(cdm_required_type):  # check that returned type is acceptable as dictated by json_python_type_mappings
                if isinstance(attribute_value, list):  # check that the nested list objects are correct
                    attribute_value_types = [type(y) for y in attribute_value]
                    assert len(set(attribute_value_types)) == 1, 'List should not contains different types. {}'.format(attribute_value)  # all the types in the list are the same
                    python_nested_type = re.search(r"<class '(.*?)'>", (str(type(attribute_value[0])))).group(1)
                    if python_nested_type not in self.json_python_type_mappings.get(cdm_required_nested_type):
                        raise ValueError(
                            ' For attribute "{}.{}" the converter retuned a list with entity types "{}" \nbut the config requires a list with entity type "{}"'.format(
                                common_entity_type, attribute_name, python_nested_type, cdm_required_nested_type))
                # types match
            else:
                raise ValueError(
                    ' For attribute "{}.{}" the converter retuned a list with entity types "{}" \nbut the config requires a list with entity type "{}"'.format(
                        common_entity_type, attribute_name, python_type, cdm_required_type))

    def sub_object_handler(self, entity, common_entity_type):
        # Attributes and Units are objects in the common data model that are nested in entries. These are created from dict here.

        for attribute_name, attribute_value in entity.items():
            # init
            translation = self.translation_config.get(common_entity_type).get(attribute_name)
            cdm_required_type = translation.get('type')  # expected object type
            cdm_required_nested_type = translation.get('items', 'None')  # expected object type in arrays
            if cdm_required_type == 'object':
                if attribute_name == 'attributes' and common_entity_type == 'sample':
                    # Special handling for sample.attributes. Requires dict with nested Attribute class object as values. This is not expressed in the config yet.
                    attribute_obj_dict = {}
                    for sub_attribute_name, sub_attribute_value in attribute_value.items():
                        assert isinstance(sub_attribute_value, dict), 'Expected dict in samples.attributes'
                        if sub_attribute_value.get('unit'):
                            sub_attribute_value['unit'] = Unit(**sub_attribute_value.get('unit'))
                        attribute_obj_dict[sub_attribute_name] = Attribute(**sub_attribute_value)
                    entity[attribute_name] = attribute_obj_dict
                else:
                    entity[attribute_name] = Attribute(**attribute_value)
        return entity


    def main(self):
        submission = defaultdict(list)

        # add entities to submission dict

        for common_entity_type, entities in self.project_translated_output.items():
            assert common_entity_type in self.object_mapping, 'Cannot map entity type {} to object'.format(common_entity_type)

            for entity in entities:
                self.validate_json_type_vs_config(entity, common_entity_type) # ensure value is correct type
                entity = self.sub_object_handler(entity, common_entity_type) # create sub nested objects
                submission[common_entity_type].append(self.object_mapping.get(common_entity_type)(**entity))

        # make the submission object

        for common_entity_type, entities in submission.items(): # specify non list for one_entity_per_project
            if ProjectImporter.get_entity_granularity(common_entity_type) == 'one_entity_per_project':
                assert len(entities) == 1, 'Submission can only contain 1 {}'.format(common_entity_type)
                submission[common_entity_type] = entities[0]
        submission['assay'] = submission.pop('singlecell_assay')  # hca project only use this assay type.

        submission['info'] = self.conversion_info # add metadata to the submission about the conversion
        submission['analysis'] = [] # hca import contain no analysis but this is a required field

        self.submission_object = Submission(**submission)

# todo sample.attribute needs special type handling as rule is not described in config. Sample attributes are a dictionary with the categories as keys and Attributes as values

# print('attribute_name {}'.format(attribute_name))
# print('attribute_value {}'.format(attribute_value))
# print('cdm_required_type {}'.format(cdm_required_type))
# print('cdm_required_nested_type {}'.format(cdm_required_nested_type))
# print('python_type {}'.format(python_type))
# print('\n')
# sys.exit()



# return Attribute(value=value,
#                          unit=unit,
#                          term_accession=term_accession,
#                          term_source=term_source)


# self.info: dict = info
# self.project: Project = project
# self.study: Study = study
# self.protocol: List[Protocol] = protocol
# self.sample: List[Sample] = sample











































# self.assay: List[Union[SeqAssay, SingleCellAssay, MicroarrayAssay]] = assay
# self.assay_data: List[AssayData] = assay_data
# self.analysis: List[Analysis] = analysis

