'''
Matches the version of hca metadata in the config (hca paths) to the version in the metadata.
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
    def __init__(self, hca_project_uuid, translation_config_file):
        # init

        with open(translation_config_file) as f1:
            self.translation_config = json.load(f1)

        property_migrations_file_url = 'https://raw.githubusercontent.com/HumanCellAtlas/metadata-schema/master/json_schema/property_migrations.json'
        filename, headers = urllib.request.urlretrieve(property_migrations_file_url,
                                                       filename='etc/property_migrations.json')



        with open(filename) as f2:
            self.property_migrations = json.load(f2)

        self.test_query = self.run_query()

        res, total_hits = get_dss_generator(hca_project_uuid)

        self.example_metadata = next(res)

        sys.exit()

    def run_query(self):
        # auth issues, waiting for response from query service.

        # url = "https://query.staging.data.humancellatlas.org/v1/"
        # querystring = {"Accept": "application/json", "Content-Type": "application/json"}
        # payload = "{\n  \"params\": {},\n  \"query\": \"SELECT fqid FROM BUNDLES LIMIT 10;\"\n}"
        # headers = {
        #     'Content-Type': "text/plain",
        #     'Accept': "*/*",
        #     'Host': "query.staging.data.humancellatlas.org",
        # }
        # response = requests.request("POST", url, data=payload, headers=headers, params=querystring)
        # return response.text

        query = 'SELECT fqid FROM BUNDLES LIMIT 10;'
        url = "https://query.staging.data.humancellatlas.org/v1/"

        data = json.dumps({'query': query})
        headers = {"Content-Type": "application/json"}
        response = requests.post(url, headers=headers, data=data, allow_redirects=False)
        print(response)