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
    # print("Updated graph RDD:")
    # for node in updated_graph_rdd.collect():
    #     print(f"Node {node.id} has color {node.color} and has neighbors {node.neighbors}")
    
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
    sorted_nodes = sorted(nodes, key=lambda x: len(x.neighbors))  # Optional: Replace with x.degree # TODO len(x.neighbors)
    for node in sorted_nodes:
        # Check if any neighbor is already colored
        if not any(neighbor.id in colored_ids for neighbor in node.neighbors):
            colored_ids.add(node.id)
            colored_nodes.append(node)
    return [(node, color) for node in colored_nodes]

# Graph coloring algorithm    
def graph_coloring(graph_rdd, numOfColors, sc):
    graph_rdd = graph_rdd.map(changeColorFirstIteration)
    
    graph_rdd = changeColorBiggestDegree(graph_rdd, 0)
    
    previous_uncolored_count = None
    
    while True:
        # Broadcast the current colors and update the neighbors--------------------------------------------
        broadcasted_colors = broadcast_colors(graph_rdd, sc)
        graph_rdd = graph_rdd.map(lambda node: update_neighbors_with_colors(node, broadcasted_colors))
        
        # Check if there are uncolored nodes
        graph_rdd_uncolored = graph_rdd.filter(lambda node: node.color == -1)
        
        uncolored_count = graph_rdd_uncolored.count()
        print(f"Uncolored nodes remaining: {uncolored_count}")
        if uncolored_count == 0:
            break
        
        if uncolored_count == previous_uncolored_count:
            graph_rdd = graph_rdd.map(lambda node: update_neighbors_with_colors(node, broadcasted_colors))
            continue
        previous_uncolored_count = uncolored_count
        
        nodes_uncolored_with_neighbor_colors = graph_rdd_uncolored.map(lambda node: (node, [neighbor.color for neighbor in node.neighbors]))
        
        nodes_with_candidate_color = nodes_uncolored_with_neighbor_colors \
                                    .map(lambda node_with_neighbors: assign_color(node_with_neighbors, numOfColors)) \
                                    .filter(lambda x: x[0] != -2)
        
        failed_nodes = nodes_with_candidate_color.filter(lambda x: x[0] == -3).count()
        
        if failed_nodes > 0:
            print(f"Graph coloring failed: {failed_nodes} nodes have no available colors.")
            return False, graph_rdd
        
        grouped_by_candidate_color = nodes_with_candidate_color.groupByKey().mapValues(list)
        
        colored_nodes_touple = grouped_by_candidate_color.flatMap(resolve_collisions)
        
        num_partitions = graph_rdd.getNumPartitions()
        
         # Partition colored_nodes_touple
        colored_nodes_touple_partitioned = colored_nodes_touple.map(lambda nc: (nc[0].id, nc[1])) \
                                                              .partitionBy(num_partitions)
        
        # Partition graph_rdd and perform join
        updated_graph_rdd = (
            graph_rdd.map(lambda node: (node.id, node))
            .partitionBy(num_partitions)
            .leftOuterJoin(colored_nodes_touple_partitioned)
            .map(color_node)
            .persist(StorageLevel.MEMORY_AND_DISK)  # Cache for reuse
        )
        
        # Update the graph RDD for the next iteration
        graph_rdd = updated_graph_rdd
        
    return True, graph_rdd

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
        graph = Graph(0, 0)  # Dummy parameters
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
