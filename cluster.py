class Cluster:
    def __init__(self):
        self.resources = {
            'cpus': ['cpu0','cpu1','cpu2','cpu3','cpu4','cpu5','cpu6','cpu7','cpu8','cpu9','cpu10','cpu11','cpu12','cpu13','cpu14','cpu15'],
            'gpus': ['gpu0','gpu1','gpu2','gpu3','gpu4','gpu5','gpu6','gpu7',]
        }

        self.power_specs = {
            'cpu': 100,
            'gpu': 300
        }

    def get_all_nodes(self):
        all_nodes = []
        for node in self.resources.values():
            all_nodes.extend(node)
        return all_nodes
    
    def get_node_type(self, node_name):
        for r_type, nodes in self.resources.items():
            if node_name in nodes:
                return 'gpu' if 'gpu' in r_type else 'cpu'
        return None
    
    def get_power_consumption(self, node_name):
        node_type = self.get_node_type(node_name)
        return self.power_specs.get(node_type, 0)

