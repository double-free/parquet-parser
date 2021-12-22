#pragma once

#include <cstdint>
#include <utility>

namespace yy {
  class OwningByteArray
  {
  public:
      OwningByteArray() : bytes_(nullptr), len_(0) {}
      OwningByteArray(const uint8_t *bytes, int64_t len)
          : bytes_(bytes), len_(len) {}

      int64_t len() const
      {
          return len_;
      }

      const uint8_t *bytes() const
      {
          return bytes_;
      }

      const char *chars() const
      {
          return reinterpret_cast<const char *>(bytes_);
      }

      // rule of 5
      ~OwningByteArray()
      {
          if (bytes_)
          {
              delete[] bytes_;
          }
      }
      // do not copy construct it, it's expensive
      OwningByteArray(const OwningByteArray &other) = delete;
      // and do not copy assign it
      OwningByteArray &operator=(const OwningByteArray &other) = delete;
      // move constructor
      OwningByteArray(OwningByteArray &&other) noexcept
      {
          bytes_ = std::exchange(other.bytes_, nullptr);
          len_ = other.len_;
      }
      // move assignment
      OwningByteArray &operator=(OwningByteArray &&other) noexcept // move assignment
      {
          std::swap(bytes_, other.bytes_);
          len_ = other.len_;
          return *this;
      }

  private:
      const uint8_t *bytes_;
      int64_t len_;
  };
} /* yy */
