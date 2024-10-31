#pragma once
#include "buzzdb.cpp"

#include <memory>
#include <vector>
#include <unordered_map>
#include <cmath>
#include <string>
#include <iostream>

//===----------------------------------------------------------------------===//
// Common Enums and Types
//===----------------------------------------------------------------------===//

enum ComparisonOperator { 
    EQ,  // Equal
    NE,  // Not Equal
    GT,  // Greater Than
    GE,  // Greater Than or Equal
    LT,  // Less Than
    LE   // Less Than or Equal
};

//===----------------------------------------------------------------------===//
// 75% Goal Implementation (Current - Basic Cost-Based Optimization)
//===----------------------------------------------------------------------===//

class TableStatistics {
private:
    size_t tuple_count = 0;
    std::unordered_map<std::string, size_t> distinct_values;
    
public:
    void updateStatistics(const std::string& column_name, const Field& value) {
        distinct_values[column_name]++;
    }

    void incrementTupleCount() { 
        tuple_count++; 
    }
    
    size_t getTupleCount() const { 
        return tuple_count; 
    }
    
    double getSelectivity(const std::string& column_name, 
                         const Field& value, 
                         ComparisonOperator op) const {
        if (distinct_values.find(column_name) == distinct_values.end()) {
            return 1.0;
        }

        double distinct_count = distinct_values.at(column_name);
        
        switch(op) {
            case EQ:
                return 1.0 / distinct_count;
            case NE:
                return 1.0 - (1.0 / distinct_count);
            case GT:
            case LT:
                return 0.3; // Estimate for range queries
            case GE:
            case LE:
                return 0.3; // Slightly higher for inclusive ranges
            default:
                return 1.0;
        }
    }
};

class CostModel {
public:
    static double estimateScanCost(size_t num_pages) {
        return num_pages * PAGE_READ_COST;
    }
    
    static double estimateJoinCost(size_t left_size, 
                                 size_t right_size, 
                                 double selectivity) {
        return (left_size + right_size) * PAGE_READ_COST * selectivity;
    }
    
private:
    static constexpr double PAGE_READ_COST = 1.0;
    static constexpr double PAGE_WRITE_COST = 2.0;
};

class PlanNode {
public:
    virtual ~PlanNode() = default;
    virtual double estimateCost() const = 0;
    virtual void execute() = 0;
};

class ScanPlanNode : public PlanNode {
private:
    BufferManager& buffer_manager;
    TableStatistics& stats;
    
public:
    ScanPlanNode(BufferManager& bm, TableStatistics& table_stats) 
        : buffer_manager(bm), stats(table_stats) {}
        
    double estimateCost() const override {
        return CostModel::estimateScanCost(buffer_manager.getNumPages());
    }
    
    void execute() override {
        ScanOperator scanner(buffer_manager);
        scanner.open();
        while (scanner.next()) {
            // Process tuples
        }
        scanner.close();
    }
};

//===----------------------------------------------------------------------===//
// 100% Goal Signatures (To Be Implemented)
//===----------------------------------------------------------------------===//

class Histogram {
public:
    // TODO: Implement histogram-based selectivity estimation
    virtual void buildHistogram(const std::vector<double>& values) = 0;
    virtual double estimateSelectivity(double value, ComparisonOperator op) = 0;
};

class EquiWidthHistogram : public Histogram {
    // TODO: Implement equi-width histogram
public:
    void buildHistogram(const std::vector<double>& values) override {};
    double estimateSelectivity(double value, ComparisonOperator op) override { return 0.0; };
};

class HashJoinPlanNode : public PlanNode {
    // TODO: Implement hash join plan node
public:
    double estimateCost() const override { return 0.0; };
    void execute() override {};
};

class SortMergeJoinPlanNode : public PlanNode {
    // TODO: Implement sort-merge join plan node
public:
    double estimateCost() const override { return 0.0; };
    void execute() override {};
};

//===----------------------------------------------------------------------===//
// 125% Goal Signatures (To Be Implemented)
//===----------------------------------------------------------------------===//

class QueryPlanCache {
public:
    // TODO: Implement plan caching
    virtual void cachePlan(const std::string& query_hash, std::unique_ptr<PlanNode> plan) = 0;
    virtual std::unique_ptr<PlanNode> lookupPlan(const std::string& query_hash) = 0;
};

class QueryRewriter {
public:
    // TODO: Implement query rewrite optimizations
    virtual void rewritePredicates(std::vector<std::unique_ptr<IPredicate>>& predicates) = 0;
};

class MultiQueryOptimizer {
public:
    // TODO: Implement multi-query optimization
    virtual void optimizeQueries(std::vector<std::unique_ptr<PlanNode>>& queries) = 0;
};


//===----------------------------------------------------------------------===//
// Query Optimizer Implementation
//===----------------------------------------------------------------------===//

class QueryOptimizer {
private:
    BufferManager& buffer_manager;
    std::unordered_map<std::string, TableStatistics> table_stats;
    
public:
    QueryOptimizer(BufferManager& bm) : buffer_manager(bm) {}
    
    void gatherStatistics() {
        ScanOperator scanner(buffer_manager);
        scanner.open();
        
        TableStatistics& stats = table_stats["main_table"];
        
        while (scanner.next()) {
            auto fields = scanner.getOutput();
            stats.incrementTupleCount();
            
            for (size_t i = 0; i < fields.size(); i++) {
                stats.updateStatistics("col_" + std::to_string(i), *fields[i]);
            }
        }
        
        scanner.close();
    }
    
    std::unique_ptr<PlanNode> optimizeQuery(
        const std::vector<std::unique_ptr<IPredicate>>& predicates) {
        return std::make_unique<ScanPlanNode>(
            buffer_manager, 
            table_stats["main_table"]
        );
    }
};

//===----------------------------------------------------------------------===//
// BuzzDB Integration
//===----------------------------------------------------------------------===//

class BuzzDB {
public:
    HashIndex hash_index;
    BufferManager buffer_manager;
    QueryOptimizer query_optimizer;

    BuzzDB() : buffer_manager(), query_optimizer(buffer_manager) {}

    void executeOptimizedQuery() {
        query_optimizer.gatherStatistics();
        
        // Create sample predicates for testing
        std::vector<std::unique_ptr<IPredicate>> predicates;
        
        // Create predicate for key > 2 AND key < 7
        auto pred_gt = std::make_unique<SimplePredicate>(
            SimplePredicate::Operand(0),  // index 0 is the key column
            SimplePredicate::Operand(std::make_unique<Field>(2)),
            SimplePredicate::ComparisonOperator::GT
        );

        auto pred_lt = std::make_unique<SimplePredicate>(
            SimplePredicate::Operand(0),
            SimplePredicate::Operand(std::make_unique<Field>(7)),
            SimplePredicate::ComparisonOperator::LT
        );

        auto complex_pred = std::make_unique<ComplexPredicate>(ComplexPredicate::AND);
        complex_pred->addPredicate(std::move(pred_gt));
        complex_pred->addPredicate(std::move(pred_lt));
        
        predicates.push_back(std::move(complex_pred));
        
        auto plan = query_optimizer.optimizeQuery(predicates);
        plan->execute();
    }

    // Existing BuzzDB methods
    bool try_to_insert(int key, int value) {
        bool status = false;
        auto num_pages = buffer_manager.getNumPages();
        for (size_t page_itr = 0; page_itr < num_pages; page_itr++) {
            auto newTuple = std::make_unique<Tuple>();

            auto key_field = std::make_unique<Field>(key);
            auto value_field = std::make_unique<Field>(value);
            float float_val = 132.04;
            auto float_field = std::make_unique<Field>(float_val);
            auto string_field = std::make_unique<Field>("buzzdb");

            newTuple->addField(std::move(key_field));
            newTuple->addField(std::move(value_field));
            newTuple->addField(std::move(float_field));
            newTuple->addField(std::move(string_field));

            auto& page = buffer_manager.getPage(page_itr);
            status = page->addTuple(std::move(newTuple));
            
            if (status == true) {
                buffer_manager.flushPage(page_itr);
                break;
            }
        }
        return status;
    }

    void insert(int key, int value) {
        if (tuple_insertion_attempt_counter >= max_number_of_tuples) {
            return;
        }

        tuple_insertion_attempt_counter += 1;
        bool status = try_to_insert(key, value);

        if (status == false) {
            buffer_manager.extend();
            bool status2 = try_to_insert(key, value);
            assert(status2 == true);
        }

        if (tuple_insertion_attempt_counter % 100 != 0) {
            auto& page = buffer_manager.getPage(0);
            page->deleteTuple(0);
            buffer_manager.flushPage(0);
        }
    }

    void scanTableToBuildIndex() {
        std::cout << "Scanning table to build index \n";
        auto num_pages = buffer_manager.getNumPages();
        size_t tuple_count = 0;

        for (size_t page_itr = 0; page_itr < num_pages; page_itr++) {
            auto& page = buffer_manager.getPage(page_itr);
            char* page_buffer = page->page_data.get();
            Slot* slot_array = reinterpret_cast<Slot*>(page_buffer);
            
            for (size_t slot_itr = 0; slot_itr < MAX_SLOTS; slot_itr++) {
                if (slot_array[slot_itr].empty == false) {
                    assert(slot_array[slot_itr].offset != INVALID_VALUE);
                    const char* tuple_data = page_buffer + slot_array[slot_itr].offset;
                    std::istringstream iss(tuple_data);
                    auto loadedTuple = Tuple::deserialize(iss);
                    int key = loadedTuple->fields[0]->asInt();
                    int value = loadedTuple->fields[1]->asInt();

                    tuple_count++;
                    hash_index.insertOrUpdate(key, value);
                }
            }
        }
        std::cout << "Tuple count: " << tuple_count << "\n";
    }

private:
    size_t max_number_of_tuples = 5000;
    size_t tuple_insertion_attempt_counter = 0;
};

int main() {
    auto start = std::chrono::high_resolution_clock::now();

    BuzzDB db;

    std::ifstream inputFile("output.txt");
    if (!inputFile) {
        std::cerr << "Unable to open file" << std::endl;
        return 1;
    }

    int field1, field2;
    while (inputFile >> field1 >> field2) {
        db.insert(field1, field2);
    }

    db.scanTableToBuildIndex();
    db.executeOptimizedQuery();

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed = end - start;
    std::cout << "Elapsed time: " << elapsed.count() << " seconds" << std::endl;

    return 0;
}