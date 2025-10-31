# Sorting Strategies in Spark (HTX xData test)

## 1. Sorting strategies

1. **Sort Within Partition (`sortWithinPartitions`)**
2. **Global Sort (`orderBy` / `sort`)**
3. **Range Partitioning + Sort**
4. **Sort-Merge Join (SMJ)**
5. **Broadcast Hash Join (BHJ)**

## 2. What to adopt here

Dataset B is small (~10k), so broadcast it and avoid sorting Dataset A. If broadcasting is disabled,
use sort-merge join on `geographical_location_oid`.
