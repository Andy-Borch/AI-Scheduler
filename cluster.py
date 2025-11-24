class Cluster:
    def __init__(self):
        # Define Heterogeneous Resources
        # "Speed" is a multiplier: 
        #   1.0 = Standard Reference
        #   5.0 = 5x Faster (Task takes 1/5th the time)
        #   0.5 = Half speed (Task takes 2x the time)
        
        self.nodes = {
            # Fast CPUs
            'cpu_fast_1': {'type': 'cpu', 'power': 200, 'speed': 2.0},
            'cpu_fast_2': {'type': 'cpu', 'power': 200, 'speed': 2.0},
            'cpu_fast_3': {'type': 'cpu', 'power': 200, 'speed': 2.0},
            'cpu_fast_4': {'type': 'cpu', 'power': 200, 'speed': 2.0},

            # Slow CPUs
            'cpu_slow_1': {'type': 'cpu', 'power': 60, 'speed': 0.8},
            'cpu_slow_2': {'type': 'cpu', 'power': 60, 'speed': 0.8},
            'cpu_slow_3': {'type': 'cpu', 'power': 60, 'speed': 0.8},
            'cpu_slow_4': {'type': 'cpu', 'power': 60, 'speed': 0.8},
            'cpu_slow_5': {'type': 'cpu', 'power': 60, 'speed': 0.8},
            'cpu_slow_6': {'type': 'cpu', 'power': 60, 'speed': 0.8},
            'cpu_slow_7': {'type': 'cpu', 'power': 60, 'speed': 0.8},
            'cpu_slow_8': {'type': 'cpu', 'power': 60, 'speed': 0.8},
            'cpu_slow_9': {'type': 'cpu', 'power': 60, 'speed': 0.8},
            'cpu_slow_10': {'type': 'cpu', 'power': 60, 'speed': 0.8},
            'cpu_slow_11': {'type': 'cpu', 'power': 60, 'speed': 0.8},
            'cpu_slow_12': {'type': 'cpu', 'power': 60, 'speed': 0.8},

            # High end GPUs like a100
            'gpu_a100_1': {'type': 'gpu', 'power': 400, 'speed': 6.0},
            'gpu_a100_2': {'type': 'gpu', 'power': 400, 'speed': 6.0},

            # Efficiency GPUs like T4
            'gpu_t4_1': {'type': 'gpu', 'power': 70, 'speed': 1.5},
            'gpu_t4_2': {'type': 'gpu', 'power': 70, 'speed': 1.5},
            'gpu_t4_3': {'type': 'gpu', 'power': 70, 'speed': 1.5},
            'gpu_t4_4': {'type': 'gpu', 'power': 70, 'speed': 1.5},
            'gpu_t4_5': {'type': 'gpu', 'power': 70, 'speed': 1.5},
            'gpu_t4_6': {'type': 'gpu', 'power': 70, 'speed': 1.5},
        }

    def get_all_nodes(self):
        return list(self.nodes.keys())

    def get_node_type(self, node_name):
        # Returns generic 'cpu' or 'gpu' for compatibility
        return self.nodes[node_name]['type']

    def get_power_consumption(self, node_name):
        return self.nodes[node_name]['power']
        
    def get_node_speed(self, node_name):
        """Returns the speed multiplier of the node."""
        return self.nodes.get(node_name, {}).get('speed', 1.0)