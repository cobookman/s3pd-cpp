cmake_minimum_required(VERSION 3.13)
project(s3pd)
set(CMAKE_CXX_STANDARD 17)

# Add homebrew libs
# link_directories("/opt/homebrew/lib")
# include_directories("/opt/homebrew/include")


# add_subdirectory(dependencies)
add_subdirectory(src)
