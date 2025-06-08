from node import Node
import json
import random

class Graph:
    def __init__(self, node_count, max_degree):
        self.nodes = self.generate_graph(node_count, max_degree)
    
    # Serialize the graph to JSON
    def serialize_graph(self, path):
        with open(path, "w") as f:
            json.dump([node.to_dict() for node in self.nodes], f, indent=4)

    # Deserialize the graph from JSON
    def deserialize_graph(self, path):
        with open(path, "r") as f:
            node_data = json.load(f)
        
        # Create nodes
        nodes = [Node(data["id"]) for data in node_data]
        
        # Link neighbors using pointers
        node_dict = {node.id: node for node in nodes}
        for node, data in zip(nodes, node_data):
            node.neighbors = [node_dict[neighbor_id] for neighbor_id in data["neighbors"]]
        
        self.nodes = nodes
        return nodes
    
    def generate_graph(self, node_count, max_degree):
        nodes = [Node(i) for i in range(node_count)]

        for node in nodes:
            degree = random.randint(0, max_degree)
            while len(node.neighbors) < degree:
                neighbor = random.choice(nodes)
                if (neighbor != node and 
                    neighbor not in node.neighbors and
                    len(neighbor.neighbors) < max_degree):
                    node.neighbors.append(neighbor)
                    neighbor.neighbors.append(node)

        return nodes
