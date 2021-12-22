# parquet-parser
C++ wrapper of [parquet-cpp](https://github.com/apache/parquet-cpp)

## Motivation

We need to access parquet files in a cpp project. Moreover, these files are in object storage and we load them directly in memory.

This tool parse parquet file in mem. Besides, it supports parsing a batch of rows (instead of col by col in primitive parquet APIs).

## Limitation

It does not handle duplicated column names.
