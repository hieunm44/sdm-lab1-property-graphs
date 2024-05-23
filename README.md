# SDM LAB 1 - Property Graphs
<div align="center">
<a href="https://www.fib.upc.edu/en">
   <img src="https://www.fib.upc.edu/sites/fib/files/images/logo-fiblletres-upc-color.svg" height=100"/>
</a>
</div>

## Overview
This repo is our project "Lab 1 - Property Graphs" in the course "Semantic Data Management (SDM)" at Universitat Politècnica de Catalunya (UPC).

## Built With
<div align="center">
<a href="https://dblp.org/">
   <img src="https://dblp.org/img/dblp.icon.192x192.png" height=40/>
</a>
<a href="https://neo4j.com/">
   <img src="https://dist.neo4j.com/wp-content/uploads/20230926084108/Logo_FullColor_RGB_TransBG.svg" height=40/>
</a>
</div>

## Setup
1. Go to https://neo4j.com/download/ and download `Neo4j Desktop`.
2. Open `Neo4j Desktop` and create a local DBMS, then set a password (in this project the password is set to `1234`).

## Usage
1. Clone the repo
   ```sh
   git clone https://github.com/hieunm44/sdm-lab1-property-graphs.git
   cd sdm-lab1-property-graphs
   ```
2. Install `Neo4j` package
   ```sh
   pip install neo4j
   ```
3. Task A2- Instantiating/Loading
   ```sh
   python3 A2.py
   ```
4. Task A3 - Evolving the graph
   ```sh
   python3 A3.py
   ```
5. Task B - Querying
   ```sh
   python3 B.py
   ```
6. Task C - Recommender
   ```sh
   python3 B.py
   ```
7. Task D - Graph algorithms
   ```sh
   python3 D.py
   ```