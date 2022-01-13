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


// void ListBuckets(const Aws::S3Crt::S3CrtClient& s3CrtClient) {
//     Aws::S3Crt::Model::ListBucketsOutcome outcome = s3CrtClient.ListBuckets();

//     if (outcome.IsSuccess()) {
//         std::cout << "All buckets under my account:" << std::endl;

//         for (auto const& bucket : outcome.GetResult().GetBuckets()) {
//             std::cout << "  * " << bucket.GetName() << std::endl;
//         }
//         std::cout << std::endl;
//         return;
//     }
//     else {
//         std::cout << "ListBuckets error:\n"<< outcome.GetError() << std::endl << std::endl;

//         return;
//     }
// }

// void ListObjects(const Aws::S3Crt::S3CrtClient& s3CrtClient, std::queue<std::string> *jobs, std::mutex *jobsMutex, bool *done) {
//     Aws::S3Crt::Model::ListObjectsV2Request request;
//     std::string bucketName = "kda-jython-test";
//     request.WithBucket(bucketName);

//     Aws::S3Crt::Model::ListObjectsV2Outcome outcome = s3CrtClient.ListObjectsV2(request);

//     while (outcome.IsSuccess()) {
//         auto continuationToken = outcome.GetResult().GetNextContinuationToken();
//         std::cout << "Objects in bucket '" << bucketName << "':" << std::endl << std::endl;

//         Aws::Vector<Aws::S3Crt::Model::Object> objects = outcome.GetResult().GetContents();
        
//         // issue next request
//         jobsMutex->lock();
//         for (Aws::S3Crt::Model::Object& object : objects) {
//             std::string objectKey = object.GetKey();
//             jobs->push(object.GetKey());
//             // std::cout << object.GetKey() << std::endl;
//         }
//         jobsMutex->unlock();
//         *done = true;
//         return;
//     }
//     std::cout << "Error: Lists Objects: " << outcome.GetError().GetMessage() << std::endl;
// }

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
    std::cout << "Getting data from: " << source << std::endl;
    std::cout << "And sending it to: " << destination << std::endl;
    S3Copy s3Copy;
    if (FLAGS_region.size() > 0) {
        s3Copy.region = FLAGS_region;
    }
    s3Copy.throughputTargetGbps = FLAGS_throughputTarget;
    s3Copy.partSize = FLAGS_partSize;
    s3Copy.concurrentDownloads = 10;
    s3Copy.Start(source, destination);
    std::cout << "DONE:!" << std::endl;
    return 0;
}
