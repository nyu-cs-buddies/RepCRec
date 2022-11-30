cmake_minimum_required(VERSION 3.10)
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

project(repcrec)
file(GLOB_RECURSE SRC_FILES src/*.cpp)
add_executable(repcrec ${SRC_FILES})
target_include_directories(repcrec PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)