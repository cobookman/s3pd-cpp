add_executable(s3pd main.cpp)
target_link_libraries(s3pd PRIVATE s3-crt)
