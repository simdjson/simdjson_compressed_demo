cmake_minimum_required(VERSION 3.16)
project(simdjson_compressed_demo CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

include(FetchContent)
FetchContent_Declare(
  simdjson
  GIT_REPOSITORY https://github.com/simdjson/simdjson.git
  GIT_TAG v5.0.1
  GIT_SHALLOW TRUE)
set(SIMDJSON_DEVELOPER_MODE OFF CACHE INTERNAL "")
FetchContent_MakeAvailable(simdjson)

find_package(ZLIB REQUIRED)

# The demo: stream gzip-compressed NDJSON through simdjson, chunk by chunk.
add_executable(gz_stream_demo src/gz_stream_demo.cpp)
target_link_libraries(gz_stream_demo PRIVATE simdjson ZLIB::ZLIB)

# Writes a large gzip-compressed NDJSON file for the demo.
add_executable(make_data src/make_data.cpp)
target_link_libraries(make_data PRIVATE simdjson ZLIB::ZLIB)

# Exhaustive test: every chunk size / every split point.
add_executable(boundary_test src/boundary_test.cpp)
target_link_libraries(boundary_test PRIVATE simdjson)

enable_testing()
add_test(NAME boundary_test COMMAND boundary_test)
