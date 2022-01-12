include(FetchContent)

add_executable(${PROJECT_NAME} main.cpp)
set_target_properties(${PROJECT_NAME} PROPERTIES RUNTIME_OUTPUT_DIRECTORY "../")

# Dependencies
## AWS SDK
set(BUILD_ONLY core;s3;s3-crt CACHE INTERNAL "")
set(BUILD_SHARED_LIBS OFF CACHE INTERNAL "")
FetchContent_Declare(
  aws-sdk-cpp
  GIT_REPOSITORY https://github.com/aws/aws-sdk-cpp.git
  GIT_TAG 1.9.171
)
FetchContent_MakeAvailable(aws-sdk-cpp)
find_package(aws-sdk-cpp)
target_link_libraries(${PROJECT_NAME} PRIVATE aws-cpp-sdk-core)
target_link_libraries(${PROJECT_NAME} PRIVATE aws-cpp-sdk-s3)
target_link_libraries(${PROJECT_NAME} PRIVATE aws-cpp-sdk-s3-crt)

# GFlags
set(GFLAGS_STRIP_INTERNAL_FLAG_HELP 1 CACHE INTERNAL "")
FetchContent_Declare(
  gflags
  GIT_REPOSITORY https://github.com/gflags/gflags.git
  GIT_TAG v2.2.2
)
FetchContent_MakeAvailable(gflags)
find_package(gflags)
target_link_libraries(${PROJECT_NAME} PRIVATE gflags::gflags)

#FetchContent_GetProperties(gflags)
#if (NOT gflags_POPULATED)
#    FetchContent_Populate(gflags)
#    add_subdirectory(${gflags_SOURCE_DIR} ${gflags_BINARY_DIR})
#endif ()

