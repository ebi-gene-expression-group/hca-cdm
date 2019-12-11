'''

func 'convert' is the main function.
It gets metadata for a hca project uuid from the datastore bundles API.
This API only returns bundes so the code iterates over bundles to build all the metadata for a DCP project.
In the future there may be better endpoints for getting metadata.
'''
# todo automatically update config with new paths to attributes as HCA schema evolves

__author__ = "hewgreen"
__license__ = "Apache 2.0"
__date__ = "30/08/2019"

from hca.dss import DSSClient
from hcacdm import aux_func as aux_func
from hcacdm.convert_entity import fetch_entity_metadata_translation
from hcacdm.make_objects import json_to_objects
from collections import defaultdict
import re
import json
import datetime
import urllib
import os
from ingest.api.ingestapi import IngestApi
import copy
import sys


NoneType = type(None)

def get_dss_generator(hca_project_uuid):
    # files.project_json.provenance.document_id project uuid you want to retreive
    # exists files.project_json.provenance.document_id removes test bundles
    # "files.analysis_process_json.type.text": "analysis" look at primary bundles only don't return analysis bundles

    query = {
        "query": {
            "bool": {
                "must": [
                    {
                        "match": {
                            "files.project_json.provenance.document_id": ""
                        }
                    },
                    {
                        "exists": {
                            "field": "files.project_json.provenance.document_id"
                        }
                    }
                ],
                "must_not": [
                    {
                        "match": {
                            "files.analysis_process_json.type.text": "analysis"
                        }
                    }
                ]
            }
        }
    }

    query.get("query").get("bool").get("must")[0].get("match")[
        "files.project_json.provenance.document_id"] = hca_project_uuid

    dss_client = DSSClient(swagger_url="https://dss.data.humancellatlas.org/v1/swagger.json")
    bundle_generator = dss_client.post_search.iterate(replica="aws", es_query=query, output_format="raw")
    total_hits = dss_client.post_search(replica="aws", es_query=query, output_format="raw").get('total_hits')
    return (bundle_generator, total_hits)

def get_hca_entity_types(ingest_api_url="http://api.ingest.data.humancellatlas.org"):
    '''
    takes a while to run so only run once per ingest.
    '''

    ingest_api = IngestApi(url=ingest_api_url)
    res = ingest_api.get_schemas(high_level_entity="type", latest_only=True)
    hca_schemas = {}
    for schema in res:
        concreteEntity = schema.get('concreteEntity')
        domainEntity = schema.get('domainEntity')
        if domainEntity not in hca_schemas:
            hca_schemas[domainEntity] = [concreteEntity + '_json']
        else:
            hca_schemas[domainEntity].append(concreteEntity + '_json')
    return hca_schemas

def get_metadata_files_by_uuid(metadata_files):
    # lookup by uuid is frequently used
    metadata_files_by_uuid = {}
    for type, file_list in metadata_files.items():
        if type != 'links_json':
            for file in file_list:
                uuid = file.get('provenance').get('document_id')
                metadata_files_by_uuid[uuid] = file
    return metadata_files_by_uuid

def config_entity_types(translation_config):
    # informational function to show the types of objects needed in the common data model read from the config.

    conf_types = set()
    for entity, i in translation_config.items():
        for attribute, v in i.items():
            try:
                conf_types.add(v['type'])
                if v['type'] =='attribute_object':
                    print(entity + '.' + attribute)
            except KeyError:
                continue
    return conf_types

def get_entity_granularity(common_entity_type):
    # common data model, hardcoded granularity assumptions not part of config at this time

    granularity={'project': 'one_entity_per_project',
                 'study': 'one_entity_per_project',
                 'publication': 'skip',
                 'contact': 'skip',
                 'sample': 'one_entity_per_hca_assay',
                 'assay_data': 'one_per_run',
                 'singlecell_assay': 'one_entity_per_hca_assay',
                 'analysis': 'skip',
                 'microarray_assay': 'skip',
                 'sequencing_assay': 'skip',
                 'data_file': 'skip',
                 'protocol': 'protocol_handling',
                 'attribute': 'skip',
                 'unit': 'skip'}
    assert common_entity_type in granularity, "{} is an unrecognised entity type. Add to the granularity dict in code.".format(common_entity_type)
    return granularity.get(common_entity_type)


def convert(hca_project_uuid, translation_config_file):

    # initialise
    logger = aux_func.get_logger(__name__)
    logger.info('Converting HCA uuid {}'.format(hca_project_uuid))
    logger.info('Using config file {}'.format(translation_config_file))

    json_config = urllib.request.urlopen(translation_config_file)
    translation_config = json.load(json_config)

    res, total_hits = get_dss_generator(hca_project_uuid)
    logger.info('Response found {} bundles in project.'.format(total_hits))
    hca_entities = get_hca_entity_types()
    project_translated_output = defaultdict(list)
    detected_protocols = []
    # converter_helper_func.conf_coverage(translation_config_file) # debug function to show hca coverage in config

    assay_links = defaultdict(list) # used to provide links in current selected assay


    for bundle in res:

        # initialize bundle
        bundle_graph = aux_func.bundle_info(bundle)
        metadata_files = bundle.get('metadata').get('files')
        metadata_files_by_uuid = get_metadata_files_by_uuid(metadata_files)
        bundle_links = defaultdict(list)
        translated_bundle_metadata = defaultdict(list)

        # check bundle assumptions
        bundle_assumption_warnings = aux_func.check_bundle_assumptions(bundle, bundle_graph).bundle_assumption_warnings
        for error_message in bundle_assumption_warnings:
            logger.warning(error_message)

        # get metadata for a bundle

        for common_entity_type, attribute_translation in translation_config.items():
            entity_granularity = get_entity_granularity(common_entity_type)
            translation_params = {
                            'bundle': bundle,
                            'common_entity_type' : common_entity_type,
                            'attribute_translation' : attribute_translation,
                            'bundle_graph' : bundle_graph,
                            'metadata_files' : metadata_files,
                            'metadata_files_by_uuid' : metadata_files_by_uuid,
                            'translation_config' : translation_config
            }

            if entity_granularity == 'one_entity_per_project':
                if common_entity_type in project_translated_output: # skip if already there
                    continue
                else:
                    common_modelling = fetch_entity_metadata_translation(translation_params)
                    translated_entity_metadata = common_modelling.translated_entity_metadata
                    translated_bundle_metadata[common_entity_type] = [translated_entity_metadata]
                    alias = translated_entity_metadata.get('alias')
                    bundle_links[common_entity_type] += [alias]

            elif entity_granularity == 'one_entity_per_hca_assay':
                # always get then add to dict or create new entry
                common_modelling = fetch_entity_metadata_translation(translation_params)
                translated_entity_metadata = common_modelling.translated_entity_metadata
                translated_bundle_metadata[common_entity_type].append(translated_entity_metadata)
                alias = translated_entity_metadata.get('alias')
                bundle_links[common_entity_type] += [alias]


            elif entity_granularity == 'protocol_handling':
                # check by alias first before grabbing all metadata. Skip if seen before.
                assert common_entity_type == 'protocol', 'This handling is only for protocols'
                if common_entity_type not in project_translated_output:
                    translated_bundle_metadata[common_entity_type] = []
                current_protocols = hca_entities.get(common_entity_type)
                protocol_files = [a for b in [c for (d, c) in {e: f for (e, f) in metadata_files.items() if e in current_protocols}.items()] for a in b]

                for file in protocol_files:
                    protocol_alias = file.get('protocol_core', None).get('protocol_id', None)
                    protocol_uuid = file.get('provenance').get('document_id')
                    bundle_links[common_entity_type] += [protocol_alias] # save link
                    assert protocol_alias, 'Hard coded assumption failed to find protocol alias. Quick fix needed.'

                    if protocol_alias not in detected_protocols:
                        detected_protocols.append(protocol_alias)
                        common_modelling = fetch_entity_metadata_translation(translation_params, protocol_uuid)
                        translated_entity_metadata = common_modelling.translated_entity_metadata
                        translated_bundle_metadata['protocol'].append(translated_entity_metadata)

            elif entity_granularity == 'one_per_run':
                '''
                WARNING This implementation will change DCP side.
                This is temp implementation.
                Hardcoded assumptions made right now for temp fix.
                Implemented because technical replicates need to be in separate assay data objects.
                This makes one assay_data object per run_index.
                It relies on the run_index being unique per bundle (process_id).
                '''

                assert common_entity_type == 'assay_data', 'This handling is only for assay_data object'
                common_modelling = fetch_entity_metadata_translation(translation_params)
                seq_files = bundle.get('metadata').get('files').get('sequence_file_json')
                assert len(seq_files) > 0, 'No sequencing files in bundle {}'.format(bundle.get('bundle_fqid'))

                lane_file_mapping = defaultdict(list)
                for x in seq_files:
                    file_name = x.get('file_core').get('file_name') # WARNING hardcoded
                    lane_index = x.get('lane_index', 'MISSING') # WARNING hardcoded
                    lane_file_mapping[lane_index].append(file_name)
                assert len(lane_file_mapping) == 1 or 'MISSING' not in list(lane_file_mapping.keys()), 'Technical replicates detected but lane_index is not provided to diccern runs.'

                seq_files_in_bundle = {x.get('name'): x for x in common_modelling.translated_entity_metadata.get('files')}

                for lane, run_grouped_files in lane_file_mapping.items():
                    lane_translated_entity_metadata = copy.deepcopy(common_modelling.translated_entity_metadata)
                    lane_translated_entity_metadata['files'] = [seq_files_in_bundle.get(a) for a in run_grouped_files]
                    lane_translated_entity_metadata['alias'] = str(lane_translated_entity_metadata.get('alias')) + '_' + str(lane)
                    translated_bundle_metadata[common_entity_type].append(lane_translated_entity_metadata)

                    alias = lane_translated_entity_metadata.get('alias')
                    bundle_links[common_entity_type] += [alias]

            elif entity_granularity == 'skip':
                # nested entities are handled at the higher level when called by the config
                # also some entities aren't used for HCA data
                continue

        # save links for the selected assay
        for common_entity_type, links in bundle_links.items():
            entity_granularity = get_entity_granularity(common_entity_type)
            if entity_granularity == 'one_entity_per_project': # add to
                assay_links[common_entity_type] += links
            elif entity_granularity == 'one_entity_per_hca_assay' or 'protocol_handling': # overwrite
                assay_links[common_entity_type] = links

        # replace placeholders with links
        # NOTE nested entities are not checked
        for common_entity_type, entities in translated_bundle_metadata.items():
            for entity in entities:
                for attribute_name, attribute_value in entity.items():
                        if (isinstance(attribute_value, str) and attribute_value.endswith('_PLACEHOLDER')) or (isinstance(attribute_value, list) and all(isinstance(x, str) for x in attribute_value) and all(x.endswith('_PLACEHOLDER') for x in attribute_value)):
                            link_to_type = re.sub(r"refs?$", '', attribute_name)
                            if link_to_type == 'assay':
                                link_to_type = 'singlecell_assay' # assumption made that in the HCA context this is always the assay type.
                            links = assay_links.get(link_to_type)
                            assert isinstance(links, (list)), 'Links must return a list of links. Assumption made of config. Links returned {}'.format(links)
                            cdm_required_type = translation_config.get(common_entity_type).get(attribute_name).get('type')
                            if cdm_required_type == 'array':
                                entity.update({attribute_name : links})
                            elif cdm_required_type == 'string':
                                assert len(links) == 1, 'Only one link should be retunred for field {}.{} but got {}'.format(common_entity_type, attribute_name, links)
                                entity.update({attribute_name: links[0]})


        # add bundle metadata to project metadata
        for entity_type, entities in translated_bundle_metadata.items():
            project_translated_output[entity_type] += entities


    # saving log

    log_dir = 'hcacdm/log/'
    json_out_filename = log_dir + hca_project_uuid + '.common_format.json'
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    with open(json_out_filename, 'w+') as f:
        json.dump(project_translated_output, f)
    logger.info('JSON saved to {}'.format(json_out_filename))

    # Convert JSON serialisable object (project_translated_output) -> CDM Python Objects

    conversion_info = {
        'conversion method': 'Automatic HCA converter',
        'input hca project uuid': hca_project_uuid,
        'conversion timestamp': datetime.datetime.now()
    }
    return json_to_objects(project_translated_output, translation_config, conversion_info).submission_object

