#include "../../include/parquet_buffer.hpp"
#include <gtest/gtest.h>

TEST(ParquetBufferTests, read) {
  const char *str = "abcdefg";
  u_char *bytes = new u_char[strlen(str)];
  std::memcpy(bytes, str, strlen(str));
  yy::OwningByteArray byteArray(bytes, strlen(str));
  yy::ParquetBuffer buffer(std::move(byteArray));

  EXPECT_EQ(7, buffer.Size());
  EXPECT_EQ(0, buffer.Tell());

  auto subBuffer = buffer.Read(2);

  EXPECT_EQ(7, buffer.Size());
  EXPECT_EQ(2, buffer.Tell());
  EXPECT_EQ("ab", std::string(reinterpret_cast<const char *>(subBuffer->data()),
                              subBuffer->size()));

  subBuffer = buffer.ReadAt(3, 4);
  EXPECT_EQ(7, buffer.Size());
  EXPECT_EQ(2, buffer.Tell());
  EXPECT_EQ("defg",
            std::string(reinterpret_cast<const char *>(subBuffer->data()),
                        subBuffer->size()));
}
