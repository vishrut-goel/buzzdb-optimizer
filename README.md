BuzzDB - Query Optimizer Implementation
A cost-based query optimizer implementation for BuzzDB, focusing on improving query execution efficiency through statistics-based plan selection and optimization strategies.
Project Structure
Copybuzzdb/
├── buzzdb.cpp      # Core database implementation
└── optimizer.cpp   # Query optimizer implementation
Current Features (75% Implementation)

Basic cost-based query optimization

Table statistics collection
Simple selectivity estimation
Cost-based scan plan selection


Core components:

Table statistics tracking (tuple counts, distinct values)
Basic cost model for operations
Query plan generation framework
Integration with BuzzDB's query execution pipeline



Build & Run
bashCopy# Compile
g++ -std=c++17 optimizer.cpp -o buzzdb

# Run
./buzzdb
Requires C++17 or later.
Usage Example
cppCopyBuzzDB db;

// Load data
db.insert(1, 100);
db.insert(2, 200);

// Build indexes
db.scanTableToBuildIndex();

// Execute optimized query
db.executeOptimizedQuery();
Planned Features
100% Goal

Histogram-based selectivity estimation
Hash join implementation
Sort-merge join implementation

125% Goal

Query plan caching
Query rewrite rules
Multi-query optimization

Testing
The implementation includes basic performance metrics:

Query execution time measurement
Statistics gathering validation
Basic plan cost estimation verification

Known Limitations

Currently supports basic scan operations
Simple selectivity estimation model
Limited join optimization

Contact
Vishrut Goel

Last updated: October 31, 2024
Project Status: Midterm Implementation (75% Goal)