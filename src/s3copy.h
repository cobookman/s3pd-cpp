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
        std::string region = Aws::Region::AWS_GLOBAL;

        // Throughput targeted per object download
        int64_t throughputTargetGbps = 10;

        // Size of range request in Bytes
        uint64_t partSize = 5 * 1024 * 1024;

        // Concurrent Object downloads
        int64_t concurrentDownloads = 10;

        // Use HTTPS?
        bool https = true;
        
        // Starts the S3 copy job
        void Start(std::string bucket, std::string prefix, std::string destination);

        // If the copy job is done
        bool IsDone();

    private:
        std::mutex jobsMutex;
        std::queue<std::string> jobs;

        std::shared_ptr<Aws::S3Crt::S3CrtClient> s3CrtClient;
        std::atomic_bool doneQueuingJobs = false;
        std::atomic_uint64_t bytesDownloaded = 0;
        std::atomic_uint64_t bytesQueued = 0;
        std::atomic_uint64_t objectsDownloaded = 0;
        std::atomic_uint64_t objectsQueued = 0;
        void queueObjects(std::string bucket, std::string prefix);
        std::string getJob();
        void worker(std::string bucket, std::string destination);

};