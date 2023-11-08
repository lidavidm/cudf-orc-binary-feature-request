cmake_minimum_required(VERSION 3.27)
project(repro LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 17)

find_package(cudf REQUIRED)
find_package(Arrow REQUIRED)
find_package(Parquet REQUIRED)

add_executable(repro repro.cpp)
target_link_libraries(repro PRIVATE cudf::cudf Arrow::arrow_shared Parquet::parquet_shared)
