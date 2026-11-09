cmake_minimum_required(VERSION 3.10.0)

set(EXECUTABLE service_cpp)

set(CMAKE_C_STANDARD 11)
set(CMAKE_CXX_STANDARD 20)

add_executable(${EXECUTABLE})

find_path(RdKafka_INCLUDE_DIR NAMES librdkafka/rdkafkacpp.h REQUIRED)
message(STATUS "====${RdKafka_INCLUDE_DIR}")

find_library(RdKafka_CPP_LIBRARY NAMES rdkafka++ REQUIRED)
find_library(RdKafka_C_LIBRARY NAMES rdkafka REQUIRED)

target_sources(${EXECUTABLE} PRIVATE service.cpp)


target_include_directories(${EXECUTABLE} PRIVATE ${RdKafka_INCLUDE_DIR})
target_link_libraries(${EXECUTABLE} PRIVATE ${RdKafka_CPP_LIBRARY} ${RdKafka_C_LIBRARY})