'''
temp script demonstrating use of functions in lib
runs converter on various hca projects (project uuids as of 09/2019)
'''

__author__ = "hewgreen"
__license__ = "Apache 2.0"
__date__ = "20/09/2019"

from hcacdm.ProjectImporter import convert

# translation_config_file = 'https://raw.githubusercontent.com/ebi-gene-expression-group/common-datamodel/master/datamodel/config/datamodel_mapping_config.json'
translation_config_file = 'https://raw.githubusercontent.com/ebi-gene-expression-group/common-datamodel/mg-ExperimentalFactor/datamodel/config/datamodel_mapping_config.json'

# HCA projects manually discovered September 2019

hca_project_uuid = 'cc95ff89-2e68-4a08-a234-480eca21ce79' # WORKING developed on this dataset
# hca_project_uuid = '008e40e8-66ae-43bb-951c-c073a2fa6774' # No error
# hca_project_uuid = 'abe1a013-af7a-45ed-8c26-f3793c24a1f4' # No error
# hca_project_uuid = 'c4077b3c-5c98-4d26-a614-246d12c2e5d7' # No error
# hca_project_uuid = '90bd6933-40c0-48d4-8d76-778c103bf545' # No error
# hca_project_uuid = '027c51c6-0719-469f-a7f5-640fe57cbece' # No error
# hca_project_uuid = 'cddab57b-6868-4be4-806f-395ed9dd635a' # No error
# hca_project_uuid = '2043c65a-1cf8-4828-a656-9e247d4e64f1' # No error
# hca_project_uuid = 'ae71be1d-ddd8-4feb-9bed-24c3ddb6e1ad' # No error
# hca_project_uuid = '74b6d569-3b11-42ef-b6b1-a0454522b4a0' # No error
# hca_project_uuid = 'f81efc03-9f56-4354-aabb-6ce819c3d414' # No error
# hca_project_uuid = '005d611a-14d5-4fbf-846e-571a1f874f70' # No error
# hca_project_uuid = '005d611a-14d5-4fbf-846e-571a1f874f70' # No error
# hca_project_uuid = '2b665444-8bef-4df2-8042-2e7b0f4c47b8' # No error
# hca_project_uuid = '091cf39b-01bc-42e5-9437-f419a66c8a45' # No error
# hca_project_uuid = 'e0009214-c0a0-4a7b-96e2-d6a83e966ce0' # No error
# hca_project_uuid = 'a9c022b4-c771-4468-b769-cabcf9738de3' # No error
# hca_project_uuid = 'f86f1ab4-1fbb-4510-ae35-3ffd752d4dfc' # No error
# hca_project_uuid = '8c3c290d-dfff-4553-8868-54ce45f4ba7f' # Taking ages
# hca_project_uuid = 'f83165c5-e2ea-4d15-a5cf-33f3550bffde' # No error
# hca_project_uuid = 'a29952d9-925e-40f4-8a1c-274f118f1f51' # hca.util.exceptions.SwaggerAPIException: None: None (HTTP 502). Details: {"message": "Internal server error"}
# hca_project_uuid = 'f8aa201c-4ff1-45a4-890e-840d63459ca2' # AssertionError: This method expects 1 file per bundle. Detected mutiple singlecell_assay entities in bundle ff87cae7-75ec-403d-b505-ae6d816ba424


# How to run the convert function

submission_object = convert(hca_project_uuid, translation_config_file)
print('Conversion of {} complete'.format(hca_project_uuid))