cmake_minimum_required(VERSION 3.12)
project(dbcore-mvp)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

enable_testing()

add_subdirectory(dbcore)
add_subdirectory(tests)

