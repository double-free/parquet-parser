cmake_minimum_required(VERSION 3.14)
set(CMAKE_CXX_STANDARD 17)

project(parquet_parser)

Option(BUILD_TEST "Build unittest" OFF)

add_library(${PROJECT_NAME} STATIC)
find_library(parquet_lib
NAMES
    "libparquet.a"
PATHS
    ${PROJECT_SOURCE_DIR}/third-party/parquet/lib/ NO_DEFAULT_PATH
)
find_library(arrow_lib
NAMES
    "libarrow.a"
PATHS
    ${PROJECT_SOURCE_DIR}/third-party/parquet/lib/ NO_DEFAULT_PATH
)
find_library(thrift_lib
NAMES
    "libthrift.a"
PATHS
    ${PROJECT_SOURCE_DIR}/third-party/parquet/lib/ NO_DEFAULT_PATH
)
find_library(zstd_lib
NAMES
    "libzstd.a"
PATHS
    ${PROJECT_SOURCE_DIR}/third-party/parquet/lib/ NO_DEFAULT_PATH
)
find_library(lz4_lib
NAMES
    "liblz4.a"
PATHS
    ${PROJECT_SOURCE_DIR}/third-party/parquet/lib/ NO_DEFAULT_PATH
)
find_library(snappy_lib
NAMES
    "libsnappy.a"
PATHS
    ${PROJECT_SOURCE_DIR}/third-party/parquet/lib/ NO_DEFAULT_PATH
)
find_library(brotli_lib
NAMES
    "libbrotlicommon.a"
PATHS
    ${PROJECT_SOURCE_DIR}/third-party/parquet/lib/ NO_DEFAULT_PATH
)
find_library(brotli_dec_lib
NAMES
    "libbrotlidec.a"
PATHS
    ${PROJECT_SOURCE_DIR}/third-party/parquet/lib/ NO_DEFAULT_PATH
)
find_library(brotli_enc_lib
NAMES
    "libbrotlienc.a"
PATHS
    ${PROJECT_SOURCE_DIR}/third-party/parquet/lib/ NO_DEFAULT_PATH
)
target_link_libraries(parquet_parser PRIVATE
    ${parquet_lib} ${arrow_lib} ${thrift_lib} ${zstd_lib}
    ${lz4_lib} ${snappy_lib} ${brotli_dec_lib} ${brotli_enc_lib} ${brotli_lib}
    boost_regex)
include_directories(parquet_parser PUBLIC
    ${PROJECT_SOURCE_DIR}/third-party/parquet/include
)
target_sources(parquet_parser PRIVATE
    src/parquet_buffer.cpp
    src/parquet_parser.cpp
)

if (BUILD_TEST)
    add_subdirectory("test")
endif()
