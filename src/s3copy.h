#include <queue>
#include <mutex>
#include <thread>
#include <aws/s3-crt/S3CrtClient.h>
#include <aws/s3-crt/model/ListObjectsV2Request.h>
#include <aws/core/Aws.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>
#include <aws/core/utils/logging/CRTLogSystem.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>
#include <atomic>

/**
 * Starts a copy job from S3 to the local filesystem
 * IS NOT THREADSAFE
 */
class S3Copy {
    public:        
        // AWS Region to use
        std::string region = Aws::Region::AWS_GLOBAL;

        // Throughput targeted per object download
        int64_t throughputTargetGbps = 10;

        // Size of range request in Bytes
        uint64_t partSize = 5 * 1024 * 1024;

        // Concurrent Object downloads
        int64_t concurrentDownloads = 10;

        // Starts the S3 copy job
        void Start(std::string s3url, std::string destination);

        // If the copy job is done
        bool IsDone();

    private:
        std::mutex jobsMutex;
        std::queue<std::string> jobs;

        Aws::S3Crt::S3CrtClient *s3CrtClient;
        std::atomic_bool doneQueuingJobs;
        void queueObjects();
        std::string getJob();
        void worker();
};