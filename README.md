# BuzzDB - Query Optimizer Implementation

A cost-based query optimizer implementation for BuzzDB, focusing on improving query execution efficiency through statistics-based plan selection and optimization strategies.

## Project Structure
    .
    ├── buzzdb.cpp                  # Core database implementation
    ├── optimzer.cpp                # Query optimizer implementation
    └── README.md

## Current Features

- **Basic cost-based query optimization**
    - Tracks number of rows and unique values
    - Helps estimate query result sizes
    - Used in cost-based optimization decisions

- **EquiWidth Histogramn**
    - Analyzes data distribution
    - Improves range query performance
    - Makes better join operation decisions

- **Query Plan Cache with LRU**
    - Remembers successful query plans
    - Reduces repeated optimization work
    - Uses LRU strategy to manage cache size

## Build & Run

```bash
# Compile
g++ -std=c++17 optimizer.cpp -o buzzdb

# Run
./buzzdb
```
## Usage Example
```cpp
Copy code
BuzzDB db;

// Load data
db.insert(1, 100);
db.insert(2, 200);

// Build indexes
db.scanTableToBuildIndex();

// Execute optimized query
db.executeOptimizedQuery();
```

## Testing
The implementation includes basic performance metrics:
- Query execution time measurement
- Statistics gathering validation
- Basic plan cost estimation verification

## Current Limitations
- Join operations are partially implemented
- Simple selectivity model for some operations
- Basic plan caching system that can be enhanced

## Contact
Vishrut Goel

Last updated: November 17, 2024
