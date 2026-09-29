cmake_minimum_required(VERSION 3.16)
project(simdjson_compressed_demo C CXX)

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

# zstd and lz4 are built from source (static), so no system package is needed.
FetchContent_Declare(
  zstd
  GIT_REPOSITORY https://github.com/facebook/zstd.git
  GIT_TAG v1.5.7
  GIT_SHALLOW TRUE
  SOURCE_SUBDIR build/cmake)
set(ZSTD_BUILD_PROGRAMS OFF CACHE INTERNAL "")
set(ZSTD_BUILD_TESTS OFF CACHE INTERNAL "")
set(ZSTD_BUILD_SHARED OFF CACHE INTERNAL "")
set(ZSTD_BUILD_STATIC ON CACHE INTERNAL "")
set(ZSTD_LEGACY_SUPPORT OFF CACHE INTERNAL "")
set(ZSTD_MULTITHREAD_SUPPORT OFF CACHE INTERNAL "")
FetchContent_Declare(
  lz4
  GIT_REPOSITORY https://github.com/lz4/lz4.git
  GIT_TAG v1.10.0
  GIT_SHALLOW TRUE
  SOURCE_SUBDIR do-not-configure) # we compile the four library files ourselves
FetchContent_MakeAvailable(zstd lz4)
add_library(lz4_frame STATIC ${lz4_SOURCE_DIR}/lib/lz4.c ${lz4_SOURCE_DIR}/lib/lz4hc.c
                             ${lz4_SOURCE_DIR}/lib/lz4frame.c ${lz4_SOURCE_DIR}/lib/xxhash.c)
target_include_directories(lz4_frame PUBLIC ${lz4_SOURCE_DIR}/lib)
add_library(frame_codecs INTERFACE)
target_link_libraries(frame_codecs INTERFACE libzstd_static lz4_frame)
target_include_directories(frame_codecs INTERFACE ${zstd_SOURCE_DIR}/lib)

find_package(ZLIB REQUIRED)
find_package(Threads REQUIRED)

# The demo: stream gzip-compressed NDJSON through simdjson, chunk by chunk.
add_executable(gz_stream_demo src/gz_stream_demo.cpp)
target_link_libraries(gz_stream_demo PRIVATE simdjson ZLIB::ZLIB frame_codecs Threads::Threads)

# Writes a large compressed NDJSON file (gzip, or zstd/lz4 in many frames) for the demo.
add_executable(make_data src/make_data.cpp)
target_link_libraries(make_data PRIVATE simdjson ZLIB::ZLIB frame_codecs)

# Exhaustive test: every chunk size / every split point.
add_executable(boundary_test src/boundary_test.cpp)
target_link_libraries(boundary_test PRIVATE simdjson Threads::Threads)

# Multi-frame zstd and lz4: every frame size, several thread counts, bad input.
add_executable(frames_test src/frames_test.cpp)
target_link_libraries(frames_test PRIVATE simdjson frame_codecs Threads::Threads)

enable_testing()
add_test(NAME boundary_test COMMAND boundary_test)
add_test(NAME frames_test COMMAND frames_test)
