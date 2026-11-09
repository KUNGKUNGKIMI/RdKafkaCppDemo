cmake_minimum_required(VERSION 3.10.0)
project(kafaka VERSION 0.1.0 LANGUAGES C CXX)


add_subdirectory(service)
add_subdirectory(client)

add_executable(kafaka main.cpp)

