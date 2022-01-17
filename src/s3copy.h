#include <queue>
#include <mutex>
#include <atomic>
#include <aws/s3-crt/S3CrtClient.h>
#include <aws/core/Aws.h>

#include <aws/core/utils/memory/AWSMemory.h>
#include <aws/core/utils/memory/stl/AWSStreamFwd.h>
#include <aws/core/utils/stream/PreallocatedStreamBuf.h>
#include <aws/core/utils/StringUtils.h>
#include <aws/core/utils/DateTime.h>
#include <aws/core/utils/threading/Executor.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>


/**
 * Starts a copy job from S3 to the local filesystem
 * IS NOT THREADSAFE
 */
class S3Copy {
    public:        
        // AWS Region to use
        std::string region;

        // Throughput targeted per object download
        int64_t throughputTargetGbps = 10;

        // Size of range request in Bytes
        uint64_t partSize = 5 * 1024 * 1024;

        // Concurrent Object downloads
        int64_t concurrentDownloads = 10;

        // Use HTTPS?
        bool https = true;

        // Discard output when set to true will throw away bytes downloaded to a noop iostream
        bool isBenchmark = false;
        
        // If set will round robin traffic across the specified interfaces
        std::string interfaces;

        // Settings for event loop
        const int32_t eventLoopThreads = 18;
        const int32_t eventLoopHosts = 8;
        const int32_t eventLoopTTL = 300;
        const int32_t cpugroup = 0;

        // Starts the S3 copy job
        void Start(std::string bucket, std::string prefix, std::string destination);

        // If the copy job is done
        bool IsDoneQueuing();

    private:
        std::mutex jobsMutex;
        std::queue<std::string> jobs;

        std::atomic_bool doneQueuingJobs = false;
        std::atomic_uint64_t bytesDownloaded = 0;
        std::atomic_uint64_t bytesQueued = 0;
        std::atomic_uint64_t objectsDownloaded = 0;
        std::atomic_uint64_t objectsQueued = 0;
        void queueObjects(std::string interface, std::string bucket, std::string prefix);
        void worker(std::string interface, std::string bucket, std::string destination);
        void printProgress();
        std::string getJob();
        std::string prepareFilepath(std::string bucket, std::string objectKey);
        Aws::S3Crt::S3CrtClient s3ClientFactory(std::string interface);
};