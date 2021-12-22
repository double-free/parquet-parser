#include "../include/parquet_parser.hpp"

using namespace yy;

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
