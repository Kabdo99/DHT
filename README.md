# Distributed Hash Table (DHT) Implementation

## Overview
This project implements a simplified Distributed Hash Table (DHT) using MPI (Message Passing Interface) in Python. The DHT provides a lookup service similar to a traditional hash table, storing key-value pairs across multiple nodes in a distributed system.

## Features
- Distributed storage of key-value pairs
- Dynamic addition and removal of storage nodes
- Circular structure for node organization
- Operations: PUT, GET, ADD, REMOVE
- Data redistribution on node addition/removal
- MPI-based communication between nodes

## Requirements
- Python 3.8+
- mpi4py library
- MPI implementation (e.g., OpenMPI, MPICH)

## Installation
1. Ensure you have Python 3.8 or later installed.
2. Install mpi4py insturctions: https://mpi4py.readthedocs.io/en/stable/install.html
3. Install an MPI implementation if not already present.

## Usage
To run the DHT program:
Where `<numProcesses>` is the total number of processes (8 for testing purposes).

## File Structure
- `dht.py`: Main implementation of the DHT
- `dht_globals.py`: Global constants and variables
- `command.py`: Implementation of the command node that runs the dht

## Implementation Details
- The DHT consists of a head node (rank 0), storage nodes (ranks 1 to numProcesses-2), and a command node (rank numProcesses-1).
- Nodes are arranged in a circular structure based on their storage IDs.
- The head node forwards commands to storage nodes and manages communication.
- Storage nodes store key-value pairs and handle data redistribution.
- The command node issues commands to the DHT system.

## Supported Operations
1. PUT: Store a key-value pair
2. GET: Retrieve a value for a given key
3. ADD: Add a new storage node to the DHT
4. REMOVE: Remove a storage node from the DHT
5. END: Terminate the DHT system

## Limitations
- The implementation assumes no errors in command syntax or semantics.
- The number of processes is fixed at 8 for testing purposes.
- The permanent storage node (ID 1000) is always present and cannot be removed.

## Contributing
This project is part of an academic assignment and is not open for external contributions.

## License
This project is for educational purposes only and is not licensed for commercial use.
