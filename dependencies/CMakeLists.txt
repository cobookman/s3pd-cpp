cmake_minimum_required(VERSION 3.13)
set(CMAKE_CXX_STANDARD 14)

include(FetchContent)

FetchContent_Declare(
  aws-sdk-cpp
  GIT_REPOSITORY https://github.com/aws/aws-sdk-cpp.git
  GIT_TAG 1.9.171
)
FetchContent_MakeAvailable(aws-sdk-cpp)

# FetchContent_Declare(
#  aws-crt-cpp
#  GIT_REPOSITORY https://github.com/awslabs/aws-crt-cpp.git
#  GIT_TAG v0.17.14
#)
#
# FetchContent_MakeAvailable(aws-crt-cpp)

