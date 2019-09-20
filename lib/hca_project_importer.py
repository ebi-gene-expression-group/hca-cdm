'''
gets metadata for a hca project uuid
'''
# todo automatically update config with new paths to attributes as HCA schema evolves

__author__ = "hewgreen"
__license__ = "Apache 2.0"
__date__ = "30/08/2019"

from hca.dss import DSSClient
from ingest.api.ingestapi import IngestApi
import lib.aux_func as aux_func
from lib.convert_entity import fetch_entity_metadata_translation
from collections import defaultdict
import re
import json

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

def check_bundle_assumptions(hca_schemas, metadata_files):
    # Atlas cannot handle multi entities in either duplicated non branched DAG or DAG bubbles
    assert 'biomaterial' in hca_schemas, 'HCA no longer have biomaterial schemas!'
    assert len(metadata_files.get('links_json', [])) == 1, 'More than one links.json file in bundle'
    assert len(metadata_files.get('project_json', [])) == 1, 'More than one project.json file in bundle'
    for biomaterial_type_entity in hca_schemas.get('biomaterial'):  # todo add support for multiple chained entities
        assert len(metadata_files.get(biomaterial_type_entity,
                                      [])) <= 1, 'More than one {} in dataset! Atlas cannot handle this at this time.'.format(
            biomaterial_type_entity)
    assert 1 <= len(metadata_files.get('sequence_file_json', [])) <= 3, 'Wrong number of sequencing files.'

    # todo add checks for assay type assumptions to flag technologies that aren't supported

def get_metadata_files_by_uuid(metadata_files):
    # lookup by uuid is frequently used
    metadata_files_by_uuid = {}
    for type, file_list in metadata_files.items():
        if type != 'links_json':
            for file in file_list:
                uuid = file.get('provenance').get('document_id')
                metadata_files_by_uuid[uuid] = file
    return metadata_files_by_uuid

def get_entity_granularity(common_entity_type):
    # common data model, hardcoded granularity assumptions not part of config at this time

    granularity={'project': 'one_entity_per_project',
                 'study': 'one_entity_per_project',
                 'publication': 'skip',
                 'contact': 'skip',
                 'sample': 'one_entity_per_hca_assay',
                 'assay_data': 'one_entity_per_hca_assay',
                 'singlecell_assay': 'one_entity_per_hca_assay',
                 'analysis': 'skip',
                 'microarray_assay': 'skip',
                 'sequencing_assay': 'skip',
                 'data_file': 'skip',
                 'protocol': 'protocol_handling'}
    assert common_entity_type in granularity, "{} is an unrecognised entity type. Add to the granularity dict in code.".format(common_entity_type)
    return granularity.get(common_entity_type)

def convert(hca_project_uuid, translation_config_file):

    # initialise
    with open(translation_config_file) as f:
        translation_config = json.load(f)
    res, total_hits = get_dss_generator(hca_project_uuid)
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
        # check_bundle_assumptions(hca_schemas, metadata_files) # todo turn on after testing

        bundle_links = defaultdict(list)
        translated_bundle_metadata = defaultdict(list)

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
        for entity_type, entities in translated_bundle_metadata.items():
            for entity in entities:
                for attribute_name, attribute_value in entity.items():
                    if isinstance(attribute_value, str) and attribute_value.endswith('_PLACEHOLDER'):
                        link_to_type = re.sub(r"refs?$", '', attribute_name)
                        entity.update({attribute_name : assay_links.get(link_to_type)})

        # add bundle metadata to project metadata
        for entity_type, entities in translated_bundle_metadata.items():
            project_translated_output[entity_type] += entities

    # temp writing out json for inspection
    with open('lib/log/' + hca_project_uuid + '.common_format.json', 'w') as f:
        json.dump(project_translated_output, f)