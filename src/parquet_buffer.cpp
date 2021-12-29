#include "../include/parquet_buffer.hpp"

#include <cassert>

using namespace yy;

ParquetBuffer::ParquetBuffer(OwningByteArray content)
    : byteArray_(std::move(content)) {}

// Returns bytes read
int64_t ParquetBuffer::Read(int64_t nbytes, uint8_t *out) {
  int64_t nread = this->ReadAt(pos_, nbytes, out);
  pos_ += nread;
  return nread;
}

std::shared_ptr<arrow::Buffer> ParquetBuffer::Read(int64_t nbytes) {
  auto buf = this->ReadAt(pos_, nbytes);
  pos_ += buf->size();
  return buf;
}

std::shared_ptr<arrow::Buffer> ParquetBuffer::ReadAt(int64_t position,
                                                     int64_t nbytes) {
  int64_t nread = std::min(byteArray_.len() - position, nbytes);
  auto buf =
      std::make_shared<arrow::Buffer>(byteArray_.bytes() + position, nread);
  return buf;
}

/// Returns bytes read
int64_t ParquetBuffer::ReadAt(int64_t position, int64_t nbytes, uint8_t *out) {
  int64_t nread = std::min(byteArray_.len() - position, nbytes);
  if (nread <= 0) {
    return 0;
  }

  memcpy(out, byteArray_.bytes() + position, nread);
  return nread;
}
