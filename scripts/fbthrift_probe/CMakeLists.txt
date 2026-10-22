# Not built. scripts/gen_third_party.py configures this project only to read
# the link line CMake computes for fbthrift's C++ runtime (and with it
# wangle, fizz, mvfst and folly), the same installation Velox builds against.
cmake_minimum_required(VERSION 3.20)
project(fbthrift_probe CXX)

find_package(wangle CONFIG REQUIRED)
find_package(FBThrift CONFIG REQUIRED)

add_executable(fbthrift_probe main.cpp)
target_link_libraries(fbthrift_probe FBThrift::thriftcpp2)
