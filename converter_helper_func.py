__author__ = "hewgreen"
__license__ = "Apache 2.0"
__date__ = "30/08/2019"

import networkx as nx

class bundle_info:
    '''
    Constructs bundle graph on the fly. This is needed to work out the order of linked entities.
    Logic for interpretation of HCA metadata graph should be done here.
    '''
    def __init__(self, bundle):
        self.bundle = bundle
        self.G = self.links_json_to_graph()
        self.top_nodes = self.find_top_nodes()
        self.ordered_biomaterials = self.get_ordered_biomaterial_list()

    def get_ordered_biomaterial_list(self):
        ordered_biomaterials = []
        ordered_nodes = list(nx.dfs_preorder_nodes(self.G, source=self.top_nodes[0]))  # todo test with diamond DAGS
        for node in ordered_nodes:
            if self.G.node[node]['entity_type'] == 'biomaterial':
                ordered_biomaterials.append(node)
        return ordered_biomaterials

    def find_top_nodes(self):
        top_nodes = []
        for node in self.G.in_degree():
            degree = node[1]
            if degree == 0:
                top_nodes.append(node[0])
                assert self.G.nodes[node[0]]['entity_type'] =='biomaterial', 'A detected top level node is not of type biomaterial'
        assert len(top_nodes) > 0, 'Bundle {} has no nodes with 0 in degrees'.format(self.bundle.get('metadata').get('uuid'))
        return top_nodes

    def links_json_to_graph(self):
        processes = self.bundle.get('metadata').get('files').get('links_json')[0].get('links')
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