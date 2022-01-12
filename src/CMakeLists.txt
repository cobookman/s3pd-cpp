include(FetchContent)

FetchContent_Declare(
  aws-sdk-cpp
  GIT_REPOSITORY https://github.com/aws/aws-sdk-cpp.git
  GIT_TAG 1.9.171
)
FetchContent_MakeAvailable(aws-sdk-cpp)
add_executable(${PROJECT_NAME} main.cpp)
find_package(aws-sdk-cpp)
target_link_libraries(${PROJECT_NAME} PRIVATE aws-cpp-sdk-s3)
target_link_libraries(${PROJECT_NAME} PRIVATE aws-cpp-sdk-core)
target_link_libraries(${PROJECT_NAME} PRIVATE aws-cpp-sdk-s3-crt)
set_target_properties(${PROJECT_NAME} PROPERTIES RUNTIME_OUTPUT_DIRECTORY "../")
