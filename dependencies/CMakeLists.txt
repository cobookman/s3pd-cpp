cmake_minimum_required(VERSION 3.13)
set(CMAKE_CXX_STANDARD 14)

include(FetchContent)

FetchContent_Declare(
  awscrt
  GIT_REPOSITORY https://github.com/awslabs/aws-crt-cpp.git
  GIT_TAG v0.17.14
)
add_subdirectory(awscrt)

