# Distributed Graph Coloring with PySpark

This project implements distributed algorithms for graph coloring using PySpark. It supports both random graph generation and loading graphs from JSON, and efficiently finds a minimal coloring using parallel processing.

## Features

- Distributed graph coloring using Apache Spark
- Supports random graph generation or loading from JSON
- Finds minimal coloring by iteratively reducing the number of colors
- Outputs coloring results to JSON
- Includes validation of coloring correctness

## Project Structure

- [`coloring.py`](coloring.py): Main graph coloring implementation (PySpark)
- [`coloring_optimized.py`](coloring_optimized.py): Optimized/alternative implementation
- [`graph.py`](graph.py): Graph and node data structures, serialization/deserialization
- [`node.py`](node.py): Node class definition
- [`test.py`](test.py): Additional tests and experiments
- [`graph.json`](graph.json): Example graph in JSON format
- [`colors.json`](colors.json): Example coloring result
- `modifikacije.pdf`: Project documentation (in Serbian)
- `__pycache__/`: Python bytecode cache

## Requirements

- Python 3.8+
- Apache Spark (PySpark)
- `pyspark` Python package

## Installation

1. Clone the repository:
    ```sh
    git clone https://github.com/danitdrvc/Distributed-Graph-Coloring-with-PySpark.git
    cd Distributed-Graph-Coloring-with-PySpark
    ```

2. Install dependencies:
    ```sh
    pip install pyspark
    ```

3. (Optional) Set up a Spark cluster or use local mode (default).

## Usage

### Color a Graph from JSON

```sh
python coloring.py --input graph.json --output-coloring colors.json
```

### Generate a Random Graph and Color It

```sh
python coloring.py --node-count 10 --max-degree 3 --output-graph graph.json --output-coloring colors.json
```

### Arguments

- `--input`: Path to input graph JSON file
- `--node-count`: Number of nodes (for random graph generation)
- `--max-degree`: Maximum degree per node (for random graph generation)
- `--output-graph`: Path to save generated graph (optional)
- `--output-coloring`: Path to save coloring result (required)

## Output

The coloring result is saved as a JSON file, e.g.:

```json
[
    {"id": 0, "color": 0},
    {"id": 1, "color": 1},
    ...
]
```

## Author

Danilo Todorovic
