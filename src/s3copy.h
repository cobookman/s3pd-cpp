#include <queue>
#include <mutex>
#include <atomic>
#include <aws/s3-crt/S3CrtClient.h>


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
        void Start(std::string bucket, std::string prefix, std::string destination);

        // If the copy job is done
        bool IsDone();

    private:
        std::mutex jobsMutex;
        std::queue<std::string> jobs;

        Aws::S3Crt::S3CrtClient *s3CrtClient;
        std::atomic_bool doneQueuingJobs;
        std::atomic_uint64_t bytesDownloaded; 
        void queueObjects(std::string bucket, std::string prefix);
        std::string getJob();
        void worker(std::string bucket, std::string destination);
};