from node import Node
from graph import Graph
import time
from pyspark.sql import SparkSession
import argparse
import json
from pyspark import StorageLevel 

def average_degree(graph_rdd):
    return graph_rdd.map(lambda node: len(node.neighbors)).mean()

def changeColorFirstIteration(node):
    if len(node.neighbors) == 0:
        node.color = 0
    else:
        node.color = -1
    return node

def changeColorBiggestDegree(graph_rdd, new_color):
    # Find the node with the maximum
    max_degree_node = graph_rdd.filter(lambda node: node.color == -1) \
                               .reduce(lambda x, y: x if len(x.neighbors) > len(y.neighbors) else y)
    
    # Update the color of the node
    max_degree_node.color = new_color
    
    # Replace the node in the RDD
    updated_graph_rdd = graph_rdd.map(
        lambda node: max_degree_node if node.id == max_degree_node.id else node
    )
    
    return updated_graph_rdd

def color_node(row):
    id, (node, color) = row
    if color is not None:
        node.color = color
    return node

# Assign colors to uncolored nodes
def assign_color(node_with_neighbors, numOfColors):
    node, neighbor_colors = node_with_neighbors
    if node.color == -1:
        used_colors = set(color for color in neighbor_colors if color != -1)
        if len(used_colors) == 0:
            return (-2, node)
        for color in range(numOfColors):
            if color not in used_colors:
                return (color, node)
        return (-3, node)
    return (-2, node)

def resolve_collisions(color_and_nodes):
    color, nodes = color_and_nodes
    if not nodes:
        return []
    # Use a set for O(1) lookups instead of a list
    colored_ids = set()
    colored_nodes = []
    # Sort nodes by ID (or degree) to minimize conflicts
    sorted_nodes = sorted(nodes, key=lambda x: len(x.neighbors))
    for node in sorted_nodes:
        # Check if any neighbor is already colored
        if not any(neighbor.id in colored_ids for neighbor in node.neighbors):
            colored_ids.add(node.id)
            colored_nodes.append(node)
    return [(node, color) for node in colored_nodes]

# Graph coloring algorithm    
def graph_coloring(graph_rdd, numOfColors, sc):
    # Initial coloring
    graph_rdd = graph_rdd.map(changeColorFirstIteration)
    #[Node(n, [neighbors], 0 or -1)]
    
    graph_rdd = changeColorBiggestDegree(graph_rdd, 0)
    #[Node(Node(n, [neighbors], color)]
    
    previous_uncolored_count = None
    
    while True:
        # Broadcast the current colors
        broadcasted_colors = broadcast_colors(graph_rdd, sc)
        # {n: color_of_node_n}
        
        
        # Create key-value pairs for more efficient processing
        node_pairs = graph_rdd.map(lambda node: (node.id, node))
        #[(n, Node(n, [neighbors], color))]
        
        
        # Extract only necessary info for uncolored nodes to reduce data shuffling
        uncolored_with_neighbor_info = node_pairs.filter(lambda x: x[1].color == -1) \
            .mapValues(lambda node: {
                "node": node,
                "neighbor_ids": [n.id for n in node.neighbors],
                "neighbor_colors": [broadcasted_colors.value.get(n.id, -1) for n in node.neighbors]
            })
        # [(node_id, {
        #     "node": Node(node_id, [neighbors], -1),
        #     "neighbor_ids": [id1, id2, ...],
        #     "neighbor_colors": [color1, color2, ...]
        # }), ...]
        
        # Count uncolored nodes
        uncolored_count = uncolored_with_neighbor_info.count()
        print(f"Uncolored nodes remaining: {uncolored_count}")
        
        if uncolored_count == 0:
            break
            
        if uncolored_count == previous_uncolored_count:
            # Update neighbors using broadcast to avoid shuffle
            graph_rdd = graph_rdd.map(lambda node: update_neighbors_with_colors(node, broadcasted_colors))
            continue
            
        previous_uncolored_count = uncolored_count
        
        # Group by potential color to reduce subsequent shuffles
        # This partitions nodes by the color they might receive
        color_assignment = uncolored_with_neighbor_info.map(
            lambda x: determine_color_key(x[1], numOfColors)
        ).filter(lambda x: x[0] != -2)  # Filter out nodes that can't be colored
        # [(potential_color, {
        #     "node": Node(node_id, [neighbors], -1),
        #     "neighbor_ids": [id1, id2, ...],
        #     "neighbor_colors": [color1, color2, ...]
        # }), ...]
        
        # Check for coloring failure
        failed_nodes = color_assignment.filter(lambda x: x[0] == -3).count()
        
        if failed_nodes > 0:
            print(f"Graph coloring failed: {failed_nodes} nodes have no available colors.")
            return False, graph_rdd
        
        # Use aggregateByKey to group nodes by color AND resolve conflicts in one operation
        colored_nodes = color_assignment.aggregateByKey(
            [],  # Initial empty accumulator
            # Combine function within partitions 
            lambda acc, node_info: resolve_conflicts_within_partition(acc, node_info),
            # Merge function across partitions
            lambda acc1, acc2: resolve_conflicts_across_partitions(acc1, acc2)
        )
        # [(color, [{
        #     "node": Node(node_id, [neighbors], -1),
        #     "neighbor_ids": [id1, id2, ...],
        #     "neighbor_colors": [color1, color2, ...]
        # }, ...]), ...]
        
        # Flatten the results into (node_id, color) pairs
        color_updates = colored_nodes.flatMap(
            lambda color_nodes: [(node["node"].id, color_nodes[0]) for node in color_nodes[1]]
        )
        # [(node_id1, color)]
        
        # Apply color updates with minimal shuffling
        num_partitions = graph_rdd.getNumPartitions()
        
        # Join efficiently with node_pairs we created earlier
        updated_graph_rdd = node_pairs.partitionBy(num_partitions) \
            .leftOuterJoin(color_updates.partitionBy(num_partitions)) \
            .map(color_node) \
            .persist(StorageLevel.MEMORY_AND_DISK)
        # [(node_id, (Node(node_id, [neighbors], color), Option[new_color])), ...] after join
        # [Node(n, [neighbors], updated_color)]
        
        # Update the graph RDD
        graph_rdd.unpersist()
        graph_rdd = updated_graph_rdd
        
    return True, graph_rdd

# Helper functions for the optimized implementation

def determine_color_key(node_info, numOfColors):
    node = node_info["node"]
    neighbor_colors = node_info["neighbor_colors"]
    
    if node.color != -1:
        return (-2, node_info)  # Already colored
        
    used_colors = set(color for color in neighbor_colors if color != -1)
    
    if len(used_colors) == 0:
        return (0, node_info)  # Use color 0 if no neighbors are colored
        
    for color in range(numOfColors):
        if color not in used_colors:
            return (color, node_info)
            
    return (-3, node_info)  # No color available

def resolve_conflicts_within_partition(accumulated_nodes, node_info):
    # Sort accumulated nodes by degree (descending)
    sorted_nodes = sorted(accumulated_nodes + [node_info], 
                          key=lambda x: len(x["neighbor_ids"]),
                          reverse=True)
    
    # Keep track of colored node IDs to check for conflicts
    colored_ids = set()
    result = []
    
    for node_data in sorted_nodes:
        # Check if this node conflicts with already colored nodes
        if not any(nid in colored_ids for nid in node_data["neighbor_ids"]):
            colored_ids.add(node_data["node"].id)
            result.append(node_data)
    
    return result

def resolve_conflicts_across_partitions(nodes1, nodes2):
    # Combine both lists and sort by degree
    combined = sorted(nodes1 + nodes2, 
                     key=lambda x: len(x["neighbor_ids"]), 
                     reverse=True)
    
    colored_ids = set()
    result = []
    
    for node_data in combined:
        if not any(nid in colored_ids for nid in node_data["neighbor_ids"]):
            colored_ids.add(node_data["node"].id)
            result.append(node_data)
    
    return result

# Broadcast current node colors
def broadcast_colors(graph_rdd, sc):
    node_colors = graph_rdd.map(lambda node: (node.id, node.color)).collectAsMap()
    return sc.broadcast(node_colors)

# Update neighbors' colors using the broadcast variable
def update_neighbors_with_colors(node, broadcast_colors):
    updated_neighbors = []
    for neighbor in node.neighbors:
        # Update the neighbor's color using the broadcasted color map
        neighbor.color = broadcast_colors.value.get(neighbor.id, -1)  # Default to -1 if not found
        updated_neighbors.append(neighbor)
    node.neighbors = updated_neighbors
    return node

def validate_graph_coloring(graph_rdd):
    # Check if there are any uncolored nodes
    uncolored_nodes = graph_rdd.filter(lambda x: x.color == -1)
    if uncolored_nodes.count() > 0:
        print(f"Graph coloring failed: {uncolored_nodes.count()} nodes have no colors.")
        return False
    
    # Check if any nodes have the same color as their neighbors
    conflicts = graph_rdd.flatMap(lambda node: [(neighbor.color, node.color) for neighbor in node.neighbors]) \
                         .filter(lambda x: x[1] == x[0])
    if conflicts.count() > 0:
        print(f"Graph coloring failed: {conflicts.count()} conflicts detected.")
        return False
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Graph Coloring CLI')
    parser.add_argument('--input', type=str, help='Input graph file (JSON)')
    parser.add_argument('--node-count', type=int, help='Number of nodes for graph generation')
    parser.add_argument('--max-degree', type=int, help='Maximum degree for graph generation')
    parser.add_argument('--output-graph', type=str, help='Output file to serialize the generated graph')
    parser.add_argument('--output-coloring', type=str, required=True, help='Output file for coloring results')
    args = parser.parse_args()

    # Load or generate graph
    if args.input:
        graph = Graph(0, 0)
        try:
            graph.nodes = graph.deserialize_graph(args.input)
        except Exception as e:
            print(f"Error loading graph: {e}")
            exit(1)
    else:
        if not args.node_count or not args.max_degree:
            parser.error("--node-count and --max-degree are required when not using --input")
        graph = Graph(args.node_count, args.max_degree)
        if args.output_graph:
            graph.serialize_graph(args.output_graph)

    # Initialize Spark
    spark = SparkSession.builder \
        .appName("Graph Coloring") \
        .master("local[*]") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.cores", "1") \
        .config("spark.default.parallelism", "1") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.driver.extraJavaOptions", "-Xss4m").getOrCreate()
    sc = spark.sparkContext

    graph_rdd = sc.parallelize(graph.nodes)
    num_partitions = graph_rdd.getNumPartitions()
    graph_rdd = (
        graph_rdd
        .map(lambda node: (node.id, node))
        .partitionBy(num_partitions, partitionFunc=lambda x: x % num_partitions)
        .map(lambda kv: kv[1])
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    # [Node(n, [neighbors], -1)]
    
    coloring_possible = True
    current_num_of_colors = args.max_degree + 1 if args.max_degree else graph_rdd.map(lambda node: len(node.neighbors)).max() + 1

    total_start_time = time.time()
    while coloring_possible:
        iteration_start = time.time()
        result, updated_graph_rdd = graph_coloring(graph_rdd, current_num_of_colors, sc)
        # Oslobađanje prethodnog RDD-a iz memorije
        graph_rdd.unpersist()
        graph_rdd = updated_graph_rdd
        
        print(f"Number of colors: {current_num_of_colors}")
        print(f"Iteration time: {time.time() - iteration_start:.2f} seconds")
        print("Validation result:", validate_graph_coloring(updated_graph_rdd))
        
        if not result:
            coloring_possible = False
            minimal_colors = current_num_of_colors + 1
        else:
            graph_rdd = updated_graph_rdd
            current_num_of_colors -= 1

    total_time = time.time() - total_start_time
    print(f"Total execution time: {total_time:.2f} seconds")
    print(f"Minimal number of colors: {minimal_colors}")

    # Save coloring results
    final_nodes = graph_rdd.collect()
    coloring_result = [{"id": node.id, "color": node.color} for node in final_nodes]
    with open(args.output_coloring, 'w') as f:
        json.dump(coloring_result, f, indent=4)

    spark.stop()
