__author__ = "hewgreen"
__license__ = "Apache 2.0"
__date__ = "30/08/2019"

import networkx as nx
import json
import logging
import os
from datetime import datetime
import sys


class bundle_info:
    '''
    Constructs bundle graph on the fly. This is needed to work out the order of linked entities.
    Logic for interpretation of HCA metadata graph should be done here.
    '''
    def __init__(self, bundle):
        self.G = self.links_json_to_graph(bundle)
        self.top_nodes = self.find_top_nodes(bundle)
        self.ordered_nodes = self.get_ordered_nodes()
        self.ordered_node_types = self.get_ordered_node_types()


        self.ordered_biomaterials = self.get_ordered_biomaterial_list()
        self.assay_process_node = self.get_assay_process_node()
        self.last_biomaterial_nodes = self.get_last_biomaterial_nodes()

    def get_last_biomaterial_nodes(self):
        '''
        Return bottom nodes connected to seq file
        '''
        last_biomaterial_nodes = list(self.G.predecessors(self.assay_process_node))
        assert all([self.G.node[x].get('entity_type') == 'biomaterial' for x in last_biomaterial_nodes]), 'Assay process has non biomaterial in degree.'
        return last_biomaterial_nodes


    def get_assay_process_node(self):
        for node in reversed(self.ordered_nodes):
            entity_type = self.G.node[node].get('entity_type')
            if entity_type != 'process': # N.B. sometimes the last node is protocol
                continue
            else:
                return node # this is the last process node

    def get_ordered_nodes(self):
        return list(nx.dfs_preorder_nodes(self.G, source=self.top_nodes[0]))

    def get_ordered_node_types(self):
        return {x: self.G.node[x].get('entity_type') for x in self.ordered_nodes}

    def get_ordered_biomaterial_list(self):
        ordered_biomaterials = []
        for node in self.ordered_nodes:
            if self.G.node[node]['entity_type'] == 'biomaterial':
                ordered_biomaterials.append(node)
        return ordered_biomaterials

    def find_top_nodes(self, bundle):
        top_nodes = []
        for node in self.G.in_degree():
            degree = node[1]
            if degree == 0:
                top_nodes.append(node[0])
                assert self.G.nodes[node[0]]['entity_type'] =='biomaterial', 'A detected top level node is not of type biomaterial'
        assert len(top_nodes) > 0, 'Bundle {} has no nodes with 0 in degrees'.format(bundle.get('metadata').get('uuid'))
        return top_nodes

    def links_json_to_graph(self, bundle):
        processes = bundle.get('metadata').get('files').get('links_json')[0].get('links')
        G = nx.DiGraph()
        node_names = {}
        node_types = {}
        for process in processes:
            process_uuid = process['process']
            input_node_uuids = process['inputs']
            output_node_uuids = process['outputs']
            protocols = process['protocols']
            for in_node in input_node_uuids:
                node_types[in_node] = process['input_type']
                G.add_edge(in_node, process_uuid)
            for out_node in output_node_uuids:
                node_types[out_node] = process['output_type']
                G.add_edge(process_uuid, out_node)
            for protocol in protocols:
                protocol_id = protocol['protocol_id']
                node_types[protocol_id] = protocol['protocol_type']
                G.add_edge(process_uuid, protocol_id)
            node_types[process_uuid] = 'process'
        nx.set_node_attributes(G, node_types, 'entity_type')
        return G

def conf_coverage(translation_config_file):
    # helper func to show which fields are covered/not covered by the config
    print('Using config: {}'.format(translation_config_file))
    config_summary = {}
    hca_mapped_counter = 0
    with open(translation_config_file) as f:
        translation_config = json.load(f)
    for entity, t in translation_config.items():
        for attribute, d in t.items():
            attribute_name = '.'.join([entity, attribute])
            try:
                hca_mapped = 'hca' in d.get('import')
            except TypeError:
                print('Attribute {} does not contain an import field'.format(attribute_name))
            if hca_mapped:
                hca_mapped_counter += 1
            else:
                print('NO HCA CONFIG MAPPING TO {}'.format(attribute_name))
            config_summary[attribute_name] = hca_mapped
    # print('{} attributes in config'.format(len(config_summary)))
    print('{}/{} attributes have hca mapping'.format(hca_mapped_counter, len(config_summary)))

def get_logger(name):
    log_format = '%(asctime)s  %(name)8s  %(levelname)5s  %(message)s'

    timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M')

    log_dir = 'hcacdm/log/'
    log_filename = log_dir + 'hcacdmConverter_' + timestamp + '.log'
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    logging.basicConfig(level=logging.DEBUG,
                        format=log_format,
                        filename=log_filename,
                        filemode='w')
    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)
    console.setFormatter(logging.Formatter(log_format))
    logging.getLogger(name).addHandler(console)
    return logging.getLogger(name)



class check_bundle_assumptions:
    '''
    Raise errors or warnings if bundles don't conform to assumptions
    This is testing data on every run rather than testing code functionality.
    Try to fail early or highlight risky conversion
    '''

    def __init__(self, bundle, bundle_graph):

        self.bundle = bundle
        self.bundle_graph = bundle_graph
        self.metadata_files = bundle.get('metadata').get('files')
        self.bundle_fqid = bundle.get('bundle_fqid')
        # self.logger = get_logger(__name__)

        bundle_assumptions = [self.not_one_links_json,
                              self.not_one_project_json,
                              self.not_one_lib_prep_json,
                              self.not_3_file_in_10x_bundle,
                              self.not_2_files_in_ss2_bundle,
                              self.more_than_one_last_biomaterial]

        self.bundle_assumption_warnings = []

        for assumption in bundle_assumptions:
            result = assumption()
            if result:
                error_message = result[0]
                severity = result[1]
                if severity == 'warning':
                    # self.logger.warning(error_message)
                    self.bundle_assumption_warnings.append(error_message)
                elif severity == 'critical':
                    raise Exception(error_message)

    # Assumption: one link file in every bundle
    def not_one_links_json(self):
        no_links_files = len(self.metadata_files.get('links_json', []))

        if no_links_files == 1:
            return False
        else:
            message = 'More than one links.json file in bundle {}'.format(self.bundle_fqid)
            severity = 'critical'
            return message, severity

    # Assumption: one project file per bundle
    def not_one_project_json(self):
        no_project_files = len(self.metadata_files.get('project_json', []))
        if no_project_files == 1:
            return False
        else:
            message = 'More than one project.json file in bundle {}'.format(self.bundle_fqid)
            severity = 'critical'
            return message, severity

    # Assumption: only one library_preparation_protocol_json per bundle
    def not_one_lib_prep_json(self):
        no_project_files = len(self.metadata_files.get('library_preparation_protocol_json', []))
        if no_project_files == 1:
            return False
        else:
            message = 'More than one library_preparation_protocol_json file in bundle {}'.format(self.bundle_fqid)
            severity = 'critical'
            return message, severity

    # Assumption: 10x v2 or v3 has 3 files in a bundle
    def not_3_file_in_10x_bundle(self):
        assay_type = self.metadata_files.get('library_preparation_protocol_json')[0].get('library_construction_method').get('ontology_label')
        no_seq_files = len(self.metadata_files.get('sequence_file_json', []))
        if no_seq_files != 3 and assay_type in ["10X v2 sequencing", "10X v3 sequencing"]:
            message = '{} bundle has {} files in bundle {}'.format(assay_type, no_seq_files, self.bundle_fqid)
            severity = 'warning'
            return message, severity
        else:
            return False

    # Assumption: SS2 has 2 files in a bundle
    def not_2_files_in_ss2_bundle(self):
        assay_type = self.metadata_files.get('library_preparation_protocol_json')[0].get('library_construction_method').get('ontology_label')
        no_seq_files = len(self.metadata_files.get('sequence_file_json', []))
        if no_seq_files != 2 and assay_type in ["Smart-seq2"]:
            message = '{} bundle has {} files in bundle {}'.format(assay_type, no_seq_files, self.bundle_fqid)
            severity = 'warning'
            return message, severity
        else:
            return False


    # Assumption: One final entity (one cell suspension)
    def more_than_one_last_biomaterial(self):
        last_biomaterial_nodes = self.bundle_graph.last_biomaterial_nodes
        if len(last_biomaterial_nodes) == 1:
            return False
        else:
            message = 'This assay ({}) has multiple biomaterials as input. This is not supported natively.'.format(self.bundle_fqid )
            severity = 'warning'
            return message, severity










