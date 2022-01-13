#include <gflags/gflags.h>
#include <iostream>
#include <queue>
#include <mutex>
#include <aws/s3-crt/S3CrtClient.h>
#include <aws/s3-crt/model/ListObjectsV2Request.h>
#include <aws/core/Aws.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>
#include <aws/core/utils/logging/CRTLogSystem.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>
#include <aws/core/utils/UUID.h>
#include "s3copy.h"

DEFINE_string(region, "", "Specifies which AWS Regional S3 API endpoint to use");
DEFINE_int64(throughputTarget, 5, "Throughput in Gbps to target for each file download, "
    "generally max instance throughput. For m5n.24xl this is generally set to 100");
DEFINE_uint64(partSize, 8 * 1024 * 1024, "Bytes to download per HTTP request");

bool isS3(std::string str) {
    return str.substr(0, 5) == "s3://";
}

std::string parseS3Bucket(std::string s3uri) {
    std::string out = "";
    // s3:// == 5 chars, starting after that prefix
    for (int i = 5; i < s3uri.length(); i ++) {
        if (s3uri[i] == '/') {
            return out;
        }
        out += s3uri[i];
    }
    return out;
}

std::string parseS3Prefix(std::string s3uri) {
    std::string out = "";
    bool isPrefix = false;
    // s3:// = 5 chars, starting after that prefix
    for (int i = 5; i < s3uri.length(); i ++) {
        if (isPrefix) {
            out += s3uri[i];
        }
        if (s3uri[i] == '/') {
            isPrefix = true;
        }
    }
    return out;
}

int main(int argc, char *argv[]) {
    // Parse flags
    gflags::SetUsageMessage("s3pd [source] [destination] ...flags");
    gflags::SetVersionString("0.0.1");

    // Parses argv for flags, and removes all the entries relating to a flag
    // More info: https://github.com/gflags/gflags/blob/827c769e5fc98e0f2a34c47cef953cc6328abced/src/gflags.h.in#L341-L346
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    gflags::ShutDownCommandLineFlags();

    // Check that we have just 2 positional arguments + 1 arg for CLI name itself
    if (argc < 3) {
        std::cout << "Missing [source] and/or [destination]" << std::endl;
        return 1;
    }
    if (argc > 3) {
        std::cout << "Utility only supports a single [source] and [destination]" << std::endl;
        return 1;
    }

    std::string source = argv[1];
    std::string destination = argv[2];
    
    if (isS3(source)) {
        std::string bucket = parseS3Bucket(source);
        std::string prefix = parseS3Prefix(source);

        std::cout << "s3 Bucket: " << bucket << std::endl;
        std::cout << "object Prefix: " << prefix << std::endl;
        std::cout << "save Destination: " << destination << std::endl;

        S3Copy s3Copy;
        if (FLAGS_region.size() > 0) {
            s3Copy.region = FLAGS_region;
        }
        s3Copy.throughputTargetGbps = FLAGS_throughputTarget;
        s3Copy.partSize = FLAGS_partSize;
        s3Copy.concurrentDownloads = 10;
        s3Copy.Start(bucket, prefix, destination);
    } else {
        std::cerr << "S3 writes not yet supported";
        return -1;
    }

    return 0;
}
