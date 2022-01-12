#include <iostream>
#include <aws/s3-crt/S3CrtClient.h>
#include <aws/core/Aws.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>
#include <aws/core/utils/logging/CRTLogSystem.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>
#include <aws/core/utils/UUID.h>



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


int main() {
    Aws::SDKOptions options;
    //Turn on logging.
    options.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Info;

    Aws::InitAPI(options);
    {

    Aws::String region = Aws::Region::US_WEST_2;
    const double throughput_target_gbps = 5;  // 1Gibps
    const uint64_t part_size = 8 * 1024 * 1024; //8MiB
    
    Aws::S3Crt::ClientConfiguration config;
    config.region = region;
    config.throughputTargetGbps = throughput_target_gbps;
    config.partSize = part_size;

    Aws::S3Crt::S3CrtClient s3_crt_client(config);

    ListBuckets(s3_crt_client);
    }
    Aws::ShutdownAPI(options);
    std::cout << "Hello World";
    return 0;
}
