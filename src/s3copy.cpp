#include "s3copy.h"
#include <fstream>
#include <unistd.h>
#include <aws/s3-crt/model/ListObjectsV2Request.h>
#include <aws/s3-crt/model/GetObjectRequest.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>
#include <aws/core/utils/logging/CRTLogSystem.h>
#include <thread>
#include <filesystem>
#include <chrono>

static const char ALLOCATION_TAG[] = "s3copy";

// no-op buffer
class SampleBuffer : public std::basic_streambuf<char> {
public:
    SampleBuffer() {
        std::cout << "SampleBuffer" << std::endl;
    }

    virtual ~SampleBuffer() {
        std::cout << "~SampleBuffer" << std::endl;
    }
};


class ChecksumStream : public Aws::IOStream {
public:
    ChecksumStream() : Aws::IOStream(&buffer) {
        std::cout << "ChecksumStream" << std::endl;
    }

    virtual ~ChecksumStream() {
        std::cout << "~ChecksumStream" << std::endl;
    }

private:
    SampleBuffer buffer;
};

void S3Copy::Start(std::string bucket, std::string prefix, std::string destination) {
    Aws::SDKOptions options;
    options.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Off;
    options.httpOptions.installSigPipeHandler = true;
    options.ioOptions.clientBootstrap_create_fn = []() {
        Aws::Crt::Io::EventLoopGroup eventLoopGroup(0, 18); // cpuGroup = 0, threads = 18
        Aws::Crt::Io::DefaultHostResolver defaultHostResolver(eventLoopGroup, 8, 300); //maxHosts = 8, maxTTL = 300
        auto clientBootstrap = Aws::MakeShared<Aws::Crt::Io::ClientBootstrap>(ALLOCATION_TAG, eventLoopGroup, defaultHostResolver);
        clientBootstrap->EnableBlockingShutdown();
        return clientBootstrap;
    };

    Aws::InitAPI(options);

    Aws::S3Crt::ClientConfiguration config;
    config.region = this->region;
    config.partSize = this->partSize;
    config.scheme = this->https ? Aws::Http::Scheme::HTTPS : Aws::Http::Scheme::HTTP;
    config.throughputTargetGbps = this->throughputTargetGbps;
    this->s3CrtClient = Aws::MakeShared<Aws::S3Crt::S3CrtClient>(ALLOCATION_TAG, config);

    // Start thread to get S3 objects in bucket & prefix
    std::thread queueThread(&S3Copy::queueObjects, this, bucket, prefix);

    // Start thread which queues s3 objects to s3crt
    std::thread workerThread(&S3Copy::worker, this, bucket, destination);

    // Output average throughput over last N seconds & progress downloading files
    const int barWidth = 70;
    const int outputEvery = 1; // seconds
    uint64_t lastObservedBytesDownloaded = 0;
    const auto start = std::chrono::system_clock::now();
    while (!this->IsDone()) {
        float progress =  (double) this->bytesDownloaded / this->bytesQueued;
        
        // Output Progress Bar
        std::cout << "[";
        int pos = barWidth * progress;
        for (int i = 0; i < barWidth; ++i) {
            if (i < pos) std::cout << "=";
            else if (i == pos) std::cout << ">";
            else std::cout << " ";
        }
        std::cout << "]";

        // Get throughput since last probe
        auto dur = std::chrono::system_clock::now() - start;
        double bytesDld = this->bytesDownloaded - lastObservedBytesDownloaded;
        lastObservedBytesDownloaded = this->bytesDownloaded;
        double throughput = (bytesDld / 1024 / 1024 / 1024) * 8 / outputEvery;

        // Output throughput & download info
        std::cout << std::fixed;
        std::cout << std::setprecision(2);
        std::cout << " " << int(progress * 100.0) << "% "
            << " [" << (double) this->bytesDownloaded / 1024 /1024 / 1024 << "" 
            << "/" << (double) this->bytesQueued / 1024 / 1024 / 1024 << "GiB] " 
            << "[" << throughput << " Gibps]"
            << "[" << this->objectsDownloaded << "/" << this->objectsQueued << "objects]"
            << "\r";
        std::cout.flush();
        sleep(outputEvery);
    }

    // Wait for threads to finish
    queueThread.join();
    workerThread.join();

    Aws::ShutdownAPI(options);
}

void S3Copy::queueObjects(std::string bucket, std::string prefix) {
    this->doneQueuingJobs = false;

    Aws::S3Crt::Model::ListObjectsV2Request request;
    request.WithBucket(bucket);
    if (prefix.length() > 0) {
        request.SetPrefix(prefix);
    }
    request.SetMaxKeys(1000);
    Aws::S3Crt::Model::ListObjectsV2Outcome outcome = this->s3CrtClient->ListObjectsV2(request);

    while (outcome.IsSuccess()) {
        // Queue up objects
        Aws::Vector<Aws::S3Crt::Model::Object> objects = outcome.GetResult().GetContents();
        {
            const std::lock_guard<std::mutex> lock(this->jobsMutex);
            for (Aws::S3Crt::Model::Object& object : objects) {
                this->bytesQueued += object.GetSize();
                this->objectsQueued += 1;
                this->jobs.push(object.GetKey());
            }
        }
        
        // Grab next page or return if done
        Aws::String continuationToken = outcome.GetResult().GetNextContinuationToken();
        if (continuationToken.size() == 0) {
            this->doneQueuingJobs = true;
            return;
        }
        request.SetContinuationToken(continuationToken);
        outcome = this->s3CrtClient->ListObjectsV2(request);
    }
    // Oh Uh, error
    this->doneQueuingJobs = true;
    std::cerr << "Error: in Lists API call: " << outcome.GetError().GetMessage() << std::endl;
    return;
}

// Blocks until either we've got no jobs or 
// until a job is scheduled
std::string S3Copy::getJob() {
    while (!this->IsDone()) {
        const std::lock_guard<std::mutex> lock(this->jobsMutex);
        if (this->jobs.size() != 0) {
            std::string objectKey = this->jobs.front();
            this->jobs.pop();
            return objectKey;
        }
    }
    return "";
}

std::string S3Copy::prepareFilepath(std::string destination, std::string objectKey) {
        std::filesystem::path path(destination);
        std::filesystem::path file(objectKey);
        std::filesystem::path full_path = path / file;

        std::filesystem::path dir_path = full_path;
        dir_path.remove_filename();

        if (!std::filesystem::exists(dir_path)) {
            std::filesystem::create_directory(dir_path);
        }

        return path.u8string();
}

void S3Copy::worker(std::string bucket, std::string destination) {
    Aws::S3Crt::Model::GetObjectRequest getRequest;
    getRequest.SetBucket(bucket);
    ChecksumStream s;
    getRequest.SetResponseStreamFactory([]() {
        return Aws::New<ChecksumStream>(ALLOCATION_TAG);
    });
    
    auto start = Aws::Utils::DateTime::Now();
    auto prev = start;
    std::atomic_uint64_t peekThroughPut = 0;
    getRequest.SetDataReceivedEventHandler([&](const Aws::Http::HttpRequest*, Aws::Http::HttpResponse*, long long bytes) {
        this->bytesDownloaded += bytes;
    });

    // Uses semaphore to limit the number of concurrent requests
    Aws::Utils::Threading::Semaphore maxConcurrent(this->concurrentDownloads, this->concurrentDownloads);
    auto GetHandler = Aws::S3Crt::GetObjectResponseReceivedHandler {
        [&](const Aws::S3Crt::S3CrtClient*, const Aws::S3Crt::Model::GetObjectRequest&, const Aws::S3Crt::Model::GetObjectOutcome &outcome,
            const std::shared_ptr<const Aws::Client::AsyncCallerContext>&)
        {
            this->objectsDownloaded += 1;
            if (!outcome.IsSuccess()) {
                std::cout << outcome.GetError() << std::endl;
            }

            maxConcurrent.Release();
        }
    };

    std::string key;
    while ((key = this->getJob()) != "") {
        maxConcurrent.WaitOne();
        getRequest.WithKey(key);
        if (!this->isBenchmark) {
            auto filepath = this->prepareFilepath(destination, key);

            getRequest.SetResponseStreamFactory([filepath]() {
                    return Aws::New<Aws::FStream>(
                        ALLOCATION_TAG, filepath, std::ios_base::out); 
            });
        }

        this->s3CrtClient->GetObjectAsync(getRequest, GetHandler, nullptr);
    }

    //     Aws::S3Crt::Model::GetObjectRequest request;
    //     request.SetBucket(bucket);
    //     request.SetKey(job);
    //     Aws::S3Crt::Model::GetObjectOutcome outcome = client->GetObject(request);
    //     if (!outcome.IsSuccess()) {
    //         std::cerr << "Error downloading object: s3://" << bucket << "/" << job << " " << outcome.GetError().GetMessage() << std::endl;
    //         continue;
    //     }

    //     std::string filename = "/tmp/tt/" + job;
    //     std::filesystem::path path(destination);
    //     std::filesystem::path file(job);
    //     std::filesystem::path full_path = path / file;
    //     std::filesystem::path dir_path = full_path;
    //     dir_path.remove_filename();

    //     // std::cout << "Saving to: " << full_path << std::endl;
    //     if (!std::filesystem::exists(dir_path)) {
    //         std::filesystem::create_directory(dir_path);
    //     }

    //     size_t bytes = outcome.GetResult().GetContentLength();
        
    //     std::ofstream output_file(full_path.c_str(), std::ios::out |std::ios::binary);
    //     output_file << outcome.GetResult().GetBody().rdbuf();
    //     this->bytesDownloaded += bytes;
    // }
}
bool S3Copy::IsDone() {
    return this->doneQueuingJobs && (this->objectsDownloaded == this->objectsQueued);
}