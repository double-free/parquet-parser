#pragma once

#include "parquet_buffer.hpp"

#include <vector>
#include <iostream>
#include <cassert>
#include <memory>
#include <map>
#include <deque>

namespace yy {
  // batched reading is more efficient than 1-by-1
  template <typename ParquetDataType>
  auto readColumnValues(
      parquet::TypedColumnReader<ParquetDataType> &typedColReader,
      int64_t len)
  {
      std::vector<typename ParquetDataType::c_type> result(len);
      assert(sizeof(typename ParquetDataType::c_type) == sizeof(result[0]));
      // is this field required?
      std::vector<int16_t> definition_levels(len);
      // can this field repeat?
      std::vector<int16_t> repetition_levels(len);

      int64_t totalReadCount = 0;
      // add this while loop for multiple parquet pages of a column
      // see http://cloudsqale.com/2020/05/29/how-parquet-files-are-written-row-groups-pages-required-memory-and-flush-operations/
      while (typedColReader.HasNext() && totalReadCount < len)
      {
        // fill values to the vector's raw data
        // readCount == 0 means reaching the end of page (but not necessarily the column)
        // set it as 1 to enter the loop, its value will be override in ReadBatch
        for (int64_t readCount = 1; readCount > 0; totalReadCount += readCount)
        {
            typedColReader.ReadBatch(
                len - totalReadCount,
                definition_levels.data() + totalReadCount,
                repetition_levels.data() + totalReadCount,
                result.data() + totalReadCount,
                &readCount);
            // std::cout << "Parsing parquet column [" << typedColReader.descr()->name()
            //           << "] expect to read [" << len << "], start from [" << totalReadCount
            //           << "], this iteration read [" << readCount << "]\n";
        }
      }

      if (totalReadCount < len)
      {
          // it must reach the end of column
          assert(typedColReader.HasNext() == false);
          result.resize(totalReadCount);
      }

      return result;
  }

  // parse an parquet file column by column
  class ParquetParser
  {
  public:
      explicit ParquetParser(std::unique_ptr<parquet::ParquetFileReader> buffer);

      std::map<std::string, std::string> getColumnType() const;

      // DType is the parquet data types
      // using BooleanType = DataType<Type::BOOLEAN>;
      // using Int32Type = DataType<Type::INT32>;
      // using Int64Type = DataType<Type::INT64>;
      // using Int96Type = DataType<Type::INT96>;
      // using FloatType = DataType<Type::FLOAT>;
      // using DoubleType = DataType<Type::DOUBLE>;
      // using ByteArrayType = DataType<Type::BYTE_ARRAY>;
      // using FLBAType = DataType<Type::FIXED_LEN_BYTE_ARRAY>;
      template <typename DType>
      std::vector<typename DType::c_type> getColumn(std::string colName)
      {
          int64_t entryCount = reader_->metadata()->num_rows();
          return getColumnBatch<DType>(colName, entryCount);
      }

      // get batch from column
      // read a whole column is memory intensive, but read row by row is slow
      // we can only read in batch
      template <typename DType>
      std::vector<typename DType::c_type> getColumnBatch(std::string colName, int64_t batchSize)
      {
          std::vector<typename DType::c_type> batchEntries;

          batchEntries.reserve(batchSize);

          int64_t readCount = 0;

          if (colReaders_.find(colName) == colReaders_.end())
          {
              // first time to read this column
              auto groupColReader = createGroupColReader(0, colName);

              assert(groupColReader.colReader != nullptr);
              colReaders_.insert({colName, groupColReader});
          }

          // use while loop to handle crossing row groups cases
          while (readCount < batchSize)
          {
              auto groupColReader = colReaders_.at(colName);
              // convertion failure will raise exception
              auto *typedColReader = dynamic_cast<parquet::TypedColumnReader<DType> *>(
                  groupColReader.colReader.get());

              if (groupColReader.colReader->HasNext() == false)
              {
                  int nextGroupId = groupColReader.groupId + 1;
                  if (nextGroupId >= reader_->metadata()->num_row_groups())
                  {
                      // no more data, break out "while" loop
                      break;
                  }
                  // no data in this group, march to next group
                  auto nextGroupColReader = createGroupColReader(nextGroupId, colName);
                  colReaders_[colName] = nextGroupColReader;

                  typedColReader = dynamic_cast<parquet::TypedColumnReader<DType> *>(
                      nextGroupColReader.colReader.get());
              }

              assert(typedColReader != nullptr);

              // read remaining
              auto entries = readColumnValues(*typedColReader, batchSize - readCount);
              readCount += entries.size();

              batchEntries.insert(batchEntries.end(), entries.begin(), entries.end());
          }

          if (readCount < batchSize)
          {
              // data tail
              // std::cout << "[ParquetParser] expected to read " << batchSize << ", actually read "
              //           << readCount << "\n";
          }

          return batchEntries;
      }

  private:
      struct PerGroupColReader
      {
          int groupId{0};
          std::shared_ptr<parquet::ColumnReader> colReader;
      };

      PerGroupColReader createGroupColReader(int groupId, std::string colName);

      std::unique_ptr<parquet::ParquetFileReader> reader_;

      // we must store the readers for each column to resume reading
      std::map<std::string, PerGroupColReader> colReaders_;
  };

  // parse each ROW into type T, with cache.
  template <typename T>
  class CachedParquetParser
  {
  public:
      explicit CachedParquetParser(std::unique_ptr<ParquetBuffer> buffer)
          : parser_(std::move(buffer))
      {
      }

      ~CachedParquetParser() {}

      // return nullopt if drained
      std::optional<T> peekNext()
      {
          if (cache_.empty())
          {
              // try to parse next if nothing in cache
              cache_ = parseNextBatch();
          }
          if (cache_.empty())
          {
              return std::nullopt;
          }

          // this is not good, it calls the default copy constructor
          // what if the T need a deep copy and did not implement?
          return cache_.front();
      }

      // please call this after "peekNext()" and make sure next is valid
      T consumeNext()
      {
          assert(peekNext() != std::nullopt);

          T result = std::move(cache_.front());
          cache_.pop_front();
          return result;
      }

  protected:
      // You need to provide implementation for this function
      // The batch size is specified by yourself when call getColumnBatch()
      virtual std::deque<T> parseNextBatch() = 0;
      ParquetParser parser_;

  private:
      std::deque<T> cache_;
  };
} /* yy */
