#include <gflags/gflags.h>
#include <iostream>
#include <aws/s3-crt/S3CrtClient.h>
#include <aws/core/Aws.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>
#include <aws/core/utils/logging/CRTLogSystem.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>
#include <aws/core/utils/UUID.h>

DEFINE_string(region, "", "Specifies which AWS Regional S3 API endpoint to use");
DEFINE_int64(throughputTarget, 5, "Throughput in Gbps to target for each file download, "
    "generally max instance throughput. For m5n.24xl this is generally set to 100");
DEFINE_uint64(partSize, 8 * 1024 * 1024, "Bytes to download per HTTP request");

bool ListBuckets(const Aws::S3Crt::S3CrtClient& s3CrtClient) {

    Aws::S3Crt::Model::ListBucketsOutcome outcome = s3CrtClient.ListBuckets();

    if (outcome.IsSuccess()) {
        std::cout << "All buckets under my account:" << std::endl;

        for (auto const& bucket : outcome.GetResult().GetBuckets())
        {
            std::cout << "  * " << bucket.GetName() << std::endl;
        }
        std::cout << std::endl;

        return true;
    }
    else {
        std::cout << "ListBuckets error:\n"<< outcome.GetError() << std::endl << std::endl;

        return false;
    }
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
        std::cout << "Missing [source] and/or [destination]";
        return 1;
    }
    if (argc > 3) {
        std::cout << "Utility only supports a single [source] and [destination]";
        return 1;
    }

    std::string source = argv[1];
    std::string destination = argv[2];
    std::cout << "Getting data from: " << source << "\n";
    std::cout << "And sending it to: " << destination << "\n";

    // Setup AWS SDK
    Aws::SDKOptions options;
    //Turn on logging.
    options.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Info;

    // CRT Options
    Aws::S3Crt::ClientConfiguration config;
    if (FLAGS_region.length() != 0) {
        config.region = FLAGS_region;
    }
    config.throughputTargetGbps = FLAGS_throughputTarget;
    config.partSize = FLAGS_partSize;

    // Code Block forces our s3 crt client to be freed up once we're done using it
    // https://docs.aws.amazon.com/sdk-for-cpp/v1/developer-guide/basic-use.html
    Aws::InitAPI(options);
    {
        Aws::S3Crt::S3CrtClient s3_crt_client(config);
        ListBuckets(s3_crt_client);
    }

    Aws::ShutdownAPI(options);
    std::cout << "Hello World";
    return 0;
}
