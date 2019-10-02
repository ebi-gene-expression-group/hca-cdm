'''
Matches the version of hca metadata in the config (hca paths) to the version in the metadata.

EARLY DEV!!!

'''

__author__ = "hewgreen"
__license__ = "Apache 2.0"
__date__ = "20/09/2019"

import json
import urllib.request
from lib.hca_project_importer import get_dss_generator
import sys
import requests

class augment_config:
    def __init__(self, hca_project_uuid, translation_config_file, property_migrations_file_url):
        # init
        filename, headers = urllib.request.urlretrieve(translation_config_file, filename='etc/translation_config.json')
        with open(filename) as f1:
            self.translation_config = json.load(f1)

        filename, headers = urllib.request.urlretrieve(property_migrations_file_url,filename='etc/property_migrations.json')
        with open(filename) as f2:
            self.property_migrations = json.load(f2)

        self.run_query()



        # res, total_hits = get_dss_generator(hca_project_uuid)
        # self.example_metadata = next(res)

        sys.exit()

    def run_query(self):

        '''
        pseudo code
        give me all the versions of entities used in a project.
        '''

        example_uuid = '622ebaa1-147d-4b8a-a97b-e7f365949fdf'

        # query = """
        #         SELECT *
        #         FROM project AS p
        #         WHERE p.uuid='622ebaa1-147d-4b8a-a97b-e7f365949fdf'
        #         LIMIT 1
        #         """

        # query = """
        #         SELECT aggregate_metadata->'project'->'hca_ingest'->>'document_id'
        #         FROM bundles AS b
        #         LIMIT 1
        #         """

        # query = """
        #         SELECT * FROM bundles LIMIT 10;
        #         """

        # query = """
        #         SELECT b.aggregate_metadata->'project'->'hca_ingest'->>'document_id' AS project_uuid
        #         FROM bundles as b
        #         LIMIT 1;
        #         """

        # query = """
        #         SELECT aggregate_metadata->'biomaterial'->'biomaterials', aggregate_metadata->'file'->'files' as f
        #         FROM bundles as b
        #         WHERE b.aggregate_metadata->'project'->'hca_ingest'->>'document_id'='4ce9972d-025d-40a7-8f43-bd48c26ab27f'
        #         LIMIT 1;
        #         """

        # query = """
        #          SELECT DISTINCT b.aggregate_metadata->'project'->'hca_ingest'->>'document_id' as project_uuid
        #          FROM bundles as b;
        #          """

        ##################################


        query = """
                 SELECT DISTINCT files->'content'->'describedBy', biomaterials->'content'->'describedBy'
                 FROM bundles as b,
                    jsonb_array_elements(aggregate_metadata->'biomaterial'->'biomaterials') as biomaterials,
                    jsonb_array_elements(aggregate_metadata->'file'->'files') as files
                 WHERE b.aggregate_metadata->'project'->'hca_ingest'->>'document_id'='374734b1-a873-4ff9-9657-b8e871b836dc';
                 """





        url = "https://query.staging.data.humancellatlas.org/v1/query"

        data = json.dumps({'query': query})
        headers = {"Content-Type": "application/json"}
        response = requests.post(url, headers=headers, data=data, allow_redirects=False)
        data = response.text
        print(data)