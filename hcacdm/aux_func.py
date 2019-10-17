__author__ = "hewgreen"
__license__ = "Apache 2.0"
__date__ = "30/08/2019"

import networkx as nx
import json

class bundle_info:
    '''
    Constructs bundle graph on the fly. This is needed to work out the order of linked entities.
    Logic for interpretation of HCA metadata graph should be done here.
    '''
    def __init__(self, bundle):
        self.G = self.links_json_to_graph(bundle)
        self.top_nodes = self.find_top_nodes(bundle)
        self.ordered_biomaterials = self.get_ordered_biomaterial_list()

    def get_ordered_biomaterial_list(self):
        ordered_biomaterials = []
        ordered_nodes = list(nx.dfs_preorder_nodes(self.G, source=self.top_nodes[0]))  # todo test with diamond DAGS
        for node in ordered_nodes:
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