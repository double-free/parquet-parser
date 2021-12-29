#include "../include/parquet_parser.hpp"

using namespace yy;

namespace yy {
  // template specification for double (which is possible to have NaN values)
  template <>
  auto readColumnValues(
      parquet::TypedColumnReader<parquet::DoubleType> &typedColReader,
      int64_t len)
  {
      std::vector<double> result(len);
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
              u_char validBits[len / sizeof(double) + 1];
              int64_t bitOffset = totalReadCount;
              int64_t readLevels = 0;
              int64_t nullCount = 0;
              typedColReader.ReadBatchSpaced(
                  len - totalReadCount,
                  definition_levels.data() + totalReadCount,
                  repetition_levels.data() + totalReadCount,
                  result.data() + totalReadCount,
                  validBits,
                  bitOffset,
                  &readLevels,
                  &readCount,
                  &nullCount);

              if (nullCount == 0)
              {
                  continue;
              }

              int64_t filledNanCount = 0;
              // there's null values
              for (int64_t i = 0; i < readCount; i++)
              {
                  if (::arrow::BitUtil::GetBit(validBits, i + bitOffset) == false)
                  {
                      filledNanCount += 1;
                      // assign NaN to null value
                      result[i + bitOffset] = std::numeric_limits<double>::quiet_NaN();
                  }
              }
              assert(filledNanCount == nullCount);
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
} /* yy */



ParquetParser::ParquetParser(std::unique_ptr<parquet::ParquetFileReader> reader)
: reader_(std::move(reader))
{
    auto colType = getColumnType();
    std::ostringstream oss;
    oss << "[ParquetParser] parquet data type: {";
    for (const auto &[colName, typeName] : colType)
    {
        oss << colName << ":" << typeName << ", ";
    }
    oss << "}\n";
    std::cout << oss.str();
}

std::map<std::string, std::string> ParquetParser::getColumnType() const
{
    std::map<std::string, std::string> columnType;

    if (reader_->metadata()->num_row_groups() == 0)
    {
        // empty
        return columnType;
    }

    auto groupReader = reader_->RowGroup(0);
    std::shared_ptr<parquet::ColumnReader> targetColReader;
    for (int i = 0; i < groupReader->metadata()->num_columns(); ++i)
    {
        auto colReader = groupReader->Column(i);
        auto colName = colReader->descr()->name();
        columnType[colName] = parquet::TypeToString(colReader->type());
    }

    return columnType;
}


ParquetParser::PerGroupColReader ParquetParser::createGroupColReader(int groupId, std::string colName)
{
    std::shared_ptr<parquet::ColumnReader> targetColReader;
    auto groupReader = reader_->RowGroup(groupId);
    for (int j = 0; j < groupReader->metadata()->num_columns(); ++j)
    {

        if (colName == groupReader->Column(j)->descr()->name())
        {
            // NOTE: we don't handle duplicated cols
            targetColReader = groupReader->Column(j);
            break;
        }
    }

    if (targetColReader == nullptr)
    {
        std::cerr << "[ParquetParser] can not find column with name [" << colName << "] \n";
        throw std::invalid_argument("[ParquetParser] can not find column with name: " + colName);
    }

    ParquetParser::PerGroupColReader groupColReader{groupId, targetColReader};
    std::cout << "[ParquetParser] in row group " << groupId << "(total "
              << reader_->metadata()->num_row_groups()
              << "), column [" << targetColReader->descr()->name() << "] is type ["
              << parquet::TypeToString(targetColReader->type()) << "]\n";
    return groupColReader;
}
