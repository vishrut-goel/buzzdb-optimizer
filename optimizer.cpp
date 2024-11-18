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

        switch (op) {
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
        : buffer_manager(bm), stats(table_stats) {
    }

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
    virtual void buildHistogram(const std::vector<double>& values) = 0;
    virtual double estimateSelectivity(double value, ComparisonOperator op) = 0;
    virtual ~Histogram() = default;
};

class EquiWidthHistogram : public Histogram {
private:
    static const size_t NUM_BUCKETS = 10;
    std::vector<size_t> bucket_counts;
    double min_value;
    double max_value;
    double bucket_width;
    size_t total_values;

public:
    EquiWidthHistogram() : bucket_counts(NUM_BUCKETS, 0), total_values(0) {}

    void buildHistogram(const std::vector<double>& values) override {
        if (values.empty()) return;

        // Initialize histogram parameters
        total_values = values.size();
        min_value = *std::min_element(values.begin(), values.end());
        max_value = *std::max_element(values.begin(), values.end());
        bucket_width = (max_value - min_value) / NUM_BUCKETS;

        // Fill buckets
        for (double value : values) {
            size_t bucket_index = getBucketIndex(value);
            bucket_counts[bucket_index]++;
        }
    }

    double estimateSelectivity(double value, ComparisonOperator op) override {
        if (total_values == 0) return 0.0;

        switch (op) {
        case EQ: {
            size_t bucket = getBucketIndex(value);
            // Assume uniform distribution within bucket
            return bucket_counts[bucket] / static_cast<double>(total_values * bucket_width);
        }
        case LT: {
            double count = 0;
            for (size_t i = 0; i < NUM_BUCKETS; i++) {
                if ((min_value + (i + 1) * bucket_width) <= value) {
                    count += bucket_counts[i];
                }
                else if (min_value + i * bucket_width <= value) {
                    // Partial bucket contribution
                    double fraction = (value - (min_value + i * bucket_width)) / bucket_width;
                    count += bucket_counts[i] * fraction;
                    break;
                }
            }
            return count / total_values;
        }
        case GT: {
            return 1.0 - estimateSelectivity(value, LT);
        }
               // TODO: Implement other comparison operators (LE, GE, NE)
        default:
            return 1.0;
        }
    }

private:
    size_t getBucketIndex(double value) const {
        if (value <= min_value) return 0;
        if (value >= max_value) return NUM_BUCKETS - 1;
        return static_cast<size_t>((value - min_value) / bucket_width);
    }
};

// Hash Join Implementation - Core functionality
class HashJoinPlanNode : public PlanNode {
private:
    std::unique_ptr<PlanNode> left_child;
    std::unique_ptr<PlanNode> right_child;
    size_t left_key_index;
    size_t right_key_index;
    TableStatistics& left_stats;
    TableStatistics& right_stats;

public:
    HashJoinPlanNode(
        std::unique_ptr<PlanNode> left,
        std::unique_ptr<PlanNode> right,
        size_t left_idx,
        size_t right_idx,
        TableStatistics& left_stat,
        TableStatistics& right_stat
    ) : left_child(std::move(left)),
        right_child(std::move(right)),
        left_key_index(left_idx),
        right_key_index(right_idx),
        left_stats(left_stat),
        right_stats(right_stat) {
    }

    double estimateCost() const override {
        // Cost model: IO cost + hash table building cost + probing cost
        double build_cost = left_child->estimateCost();
        double probe_cost = right_child->estimateCost();
        double hash_table_cost = left_stats.getTupleCount() * 1.2; // 20% overhead for hash table
        return build_cost + probe_cost + hash_table_cost;
    }

    void execute() override {
        // TODO: Implement full hash join execution
        // Current implementation focuses on the algorithm structure

        // Build phase
        std::unordered_multimap<Field, std::vector<Field>> hash_table;

        // Probe phase
        // TODO: Implement probe phase
        // 1. Scan right relation
        // 2. Probe hash table for matches
        // 3. Output matching tuples
    }
};

// Sort-Merge Join - Basic structure with TODOs
class SortMergeJoinPlanNode : public PlanNode {
public:
    double estimateCost() const override {
        // TODO: Implement cost estimation
        // Should consider:
        // 1. Cost of sorting both relations
        // 2. Cost of merge phase
        // 3. IO costs
        return 0.0;
    }

    void execute() override {
        // TODO: Implement sort-merge join
        // 1. Sort both relations
        // 2. Merge phase
        // 3. Handle duplicates
    }
};

//===----------------------------------------------------------------------===//
// 125% Goal Signatures (To Be Implemented)
//===----------------------------------------------------------------------===//

class QueryPlanCache {
private:
    struct CacheEntry {
        std::unique_ptr<PlanNode> plan;
        std::chrono::steady_clock::time_point last_access;
    };

    // Maximum number of plans to cache
    static const size_t MAX_CACHE_SIZE = 100;

    // Main cache storage: query hash -> (plan, timestamp)
    std::unordered_map<std::string, CacheEntry> cache;

    // LRU tracking: ordered list of query hashes by access time
    std::list<std::string> lru_list;

    // Helper map for O(1) access to LRU list positions
    std::unordered_map<std::string, std::list<std::string>::iterator> lru_map;

public:
    void cachePlan(const std::string& query_hash, std::unique_ptr<PlanNode> plan) {
        // If plan already exists, update it
        if (cache.find(query_hash) != cache.end()) {
            updateLRU(query_hash);
            cache[query_hash].plan = std::move(plan);
            return;
        }

        // If cache is full, evict least recently used entry
        if (cache.size() >= MAX_CACHE_SIZE) {
            // Get least recently used query hash
            const std::string& lru_hash = lru_list.back();

            // Remove from all data structures
            cache.erase(lru_hash);
            lru_map.erase(lru_hash);
            lru_list.pop_back();
        }

        // Add new plan to cache
        lru_list.push_front(query_hash);
        lru_map[query_hash] = lru_list.begin();
        cache[query_hash] = CacheEntry{
            std::move(plan),
            std::chrono::steady_clock::now()
        };
    }

    std::unique_ptr<PlanNode> lookupPlan(const std::string& query_hash) {
        auto it = cache.find(query_hash);
        if (it == cache.end()) {
            return nullptr;  // Cache miss
        }

        // Update access time and LRU order
        updateLRU(query_hash);
        it->second.last_access = std::chrono::steady_clock::now();

        // Clone the plan before returning
        // Note: In practice, you'd need to implement proper plan cloning
        return clonePlan(it->second.plan.get());
    }

    // For debugging and testing
    size_t size() const {
        return cache.size();
    }

    bool contains(const std::string& query_hash) const {
        return cache.find(query_hash) != cache.end();
    }

private:
    void updateLRU(const std::string& query_hash) {
        // Move accessed item to front of LRU list
        auto lru_it = lru_map[query_hash];
        lru_list.erase(lru_it);
        lru_list.push_front(query_hash);
        lru_map[query_hash] = lru_list.begin();
    }

    std::unique_ptr<PlanNode> clonePlan(const PlanNode* original) {
        // TODO: Implement proper plan cloning
        // This is a placeholder - in real implementation, you'd need to:
        // 1. Identify the concrete type of the plan node
        // 2. Create a new instance of that type
        // 3. Deep copy all relevant data
        // 4. Recursively clone any child plans
        return nullptr;  // Placeholder
    }
};

class QueryRewriter {
public:
    void rewritePredicates(std::vector<std::unique_ptr<IPredicate>>& predicates) {
        // TODO: Implement predicate rewriting
        // 1. Constant folding
        // 2. Predicate pushdown
        // 3. Combine predicates
    }
};

class MultiQueryOptimizer {
public:
    void optimizeQueries(std::vector<std::unique_ptr<PlanNode>>& queries) {
        // TODO: Implement multi-query optimization
        // 1. Identify common subexpressions
        // 2. Materialize common results
        // 3. Optimize execution order
    }
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