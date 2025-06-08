class Node:
    def __init__(self, id, neighbors = None, color = -1):
        self.id = id
        self.neighbors = neighbors if neighbors else []
        self.color = color
        
    # Serialization
    def to_dict(self):
        return {
            "id": self.id, 
            "neighbors": [neighbor.id for neighbor in self.neighbors], 
            "color": self.color
            }

    # Deserialization
    @staticmethod
    def from_dict(data):
        return Node(data["id"], [], data["color"])
