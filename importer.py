'''
gets metadata for a hca project uuid
'''

__author__ = "hewgreen"
__license__ = "Apache 2.0"
__date__ = "30/08/2019"

from hca.dss import DSSClient
import json
import sys
from ingest.api.ingestapi import IngestApi
import converter_helper_func
from get_common_model_entity_metadata import fetch_metadata


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

def check_bundle_assumptions(hca_schemas):
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

if __name__ == '__main__':

    # temp params
    hca_project_uuid = 'cc95ff89-2e68-4a08-a234-480eca21ce79'
    translation_config_file = './mapping_HCA_to_datamodel.json'
    with open(translation_config_file) as f:
        translation_config = json.load(f)

    # initialise
    get_generator = get_dss_generator(hca_project_uuid)
    res = get_generator[0]
    total_hits = get_generator[1]
    # hca_schemas = get_hca_entity_types() # todo turn on for prod

    # common data model, hardcoded granularity assumptions
    granularity={
    'one_entity_per_project' : ['project', 'study', 'publication', 'contact'],  # read once then skip
    'one_entity_per_hca_assay' : ['sample', 'assay', 'assay_data'],  # read as new for every hca bundle
    'unique_project_wide' : ['protocol']  # check for new ones every bundle and add to list if novel
    }

    project_translated_output = {}

    for bundle in res:

        bundle_info = converter_helper_func.bundle_info(bundle)
        metadata_files = bundle.get('metadata').get('files')
        metadata_files_by_uuid = get_metadata_files_by_uuid(metadata_files)
        # check_bundle_assumptions(hca_schemas) # todo turn on after testing

        for common_entity_type, attribute_translation in translation_config.items():

            # different entity granularity handling due to hardcoded granularity assumptions
            if common_entity_type in granularity['one_entity_per_project']:
                if common_entity_type in project_translated_output:
                    continue
                else:
                    project_translated_output[common_entity_type] = fetch_metadata(bundle, common_entity_type, attribute_translation, bundle_info, metadata_files, metadata_files_by_uuid, granularity).translated_entity_metadata
            elif common_entity_type in granularity['one_entity_per_hca_assay']:
                if common_entity_type in project_translated_output:
                    project_translated_output[common_entity_type] = {
                    **project_translated_output.get(common_entity_type),
                    **fetch_metadata(bundle, common_entity_type, attribute_translation, bundle_info, metadata_files, metadata_files_by_uuid, granularity).translated_entity_metadata}
                else:
                    project_translated_output[common_entity_type] = fetch_metadata(bundle, common_entity_type, attribute_translation, bundle_info, metadata_files, metadata_files_by_uuid, granularity).translated_entity_metadata

            elif common_entity_type in granularity['unique_project_wide']:
                continue  # todo loop through protocols in HCA bundle making a new entity if not already in bundle_translated_output

            # todo automatically update config with new paths to attributes as HCA schema evolves
