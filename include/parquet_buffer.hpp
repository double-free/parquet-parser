#pragma once
#include "owning_byte_array.hpp"
#include "parquet/api/reader.h"

namespace yy {
// this class owns the data
// use parquet::ParquetFileReader::Open() to create a parquet reader with buffer
class ParquetBuffer : public parquet::RandomAccessSource {
public:
  explicit ParquetBuffer(OwningByteArray content);

public:
  //////////////////////////////////
  // Implement RandomAccessSource //
  //////////////////////////////////
  int64_t Size() const final { return byteArray_.len(); }

  // Returns bytes read
  int64_t Read(int64_t nbytes, uint8_t *out) final;

  std::shared_ptr<arrow::Buffer> Read(int64_t nbytes) final;

  std::shared_ptr<arrow::Buffer> ReadAt(int64_t position, int64_t nbytes) final;

  /// Returns bytes read
  int64_t ReadAt(int64_t position, int64_t nbytes, uint8_t *out) final;

  void Close() final {}

  int64_t Tell() final { return pos_; }

private:
  const OwningByteArray byteArray_;

  int64_t pos_{0};
};
} // namespace yy
