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
#include <string>
#include <sstream>


static const char ALLOCATION_TAG[] = "s3copy";
static const int CPU_GROUP = 0;

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

void S3Copy::printProgress() {
    // Output average throughput over last N seconds & progress downloading files
    const int barWidth = 70;
    const int outputEvery = 1; // seconds
    uint64_t lastObservedBytesDownloaded = 0;
    const auto start = std::chrono::system_clock::now();
    while (!this->IsDoneQueuing()) {
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
}

void S3Copy::Start(std::string bucket, std::string prefix, std::string destination) {
    // Initialize our event loop and AWS API
    Aws::SDKOptions options;
    options.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Debug;
    options.httpOptions.installSigPipeHandler = true;
    options.ioOptions.clientBootstrap_create_fn = [&]() {
        Aws::Crt::Io::EventLoopGroup eventLoopGroup(this->cpugroup, this->eventLoopThreads); // cpuGroup = 0, threads = 18
        Aws::Crt::Io::DefaultHostResolver defaultHostResolver(eventLoopGroup, this->eventLoopHosts, this->eventLoopTTL); //maxHosts = 8, maxTTL = 300
        auto clientBootstrap = Aws::MakeShared<Aws::Crt::Io::ClientBootstrap>(ALLOCATION_TAG, eventLoopGroup, defaultHostResolver);
        clientBootstrap->EnableBlockingShutdown();
        return clientBootstrap;
    };
    Aws::InitAPI(options);
    
    // Start thread to get S3 objects in bucket & prefix
    std::thread queueThread(&S3Copy::queueObjects, this, "", bucket, prefix);

    // Start up our worker threads
    std::list<std::thread*> workers;
    if (this->interfaces.length() == 0) {
        workers.push_back(new std::thread(&S3Copy::worker, this, "", bucket, destination));
    } else {
        std::string interface;
        std::istringstream in(this->interfaces);
        while(std::getline(in, interface, ',')) {
            std::cout << "Starting worker on interface: " << interface << std::endl;
            workers.push_back(new std::thread(&S3Copy::worker, this, interface, bucket, destination));
        }
    }

    printProgress();

    // Block and wait for all threads to exit
    // NOTE: worker threads could still be working on their last async GET requests
    queueThread.join();
    while (!workers.empty()) {
        workers.front()->join();
        workers.pop_front();
    }

    // Shutdown AWS API
    Aws::ShutdownAPI(options); 
}

void S3Copy::queueObjects(std::string interface, std::string bucket, std::string prefix) {
    this->doneQueuingJobs = false;

    Aws::S3Crt::S3CrtClient s3CrtClient = this->s3ClientFactory(interface);
    Aws::S3Crt::Model::ListObjectsV2Request request;
    request.WithBucket(bucket);
    if (prefix.length() > 0) {
        request.SetPrefix(prefix);
    }
    request.SetMaxKeys(1000);

    Aws::S3Crt::Model::ListObjectsV2Outcome outcome = s3CrtClient.ListObjectsV2(request);
    while (outcome.IsSuccess()) {
        // Add listed objects to our queue
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
        outcome = s3CrtClient.ListObjectsV2(request);
    }

    // Oh Uh, error
    this->doneQueuingJobs = true;
    std::cerr << "Error: in Lists API call: " << outcome.GetError().GetMessage() << std::endl;
    return;
}

// Get's a job from the queue.
// Will wait and block if our queue is empty, but we're not yet finished
// our list/queueObjects operation
std::string S3Copy::getJob() {
    while (!this->IsDoneQueuing()) {
        const std::lock_guard<std::mutex> lock(this->jobsMutex);
        if (this->jobs.size() != 0) {
            std::string objectKey = this->jobs.front();
            this->jobs.pop();
            return objectKey;
        }
    }
    return "";
}

// Returns a complete path for where to save the file, and creates any missing directories
std::string S3Copy::prepareFilepath(std::string destination, std::string objectKey) {
        std::filesystem::path path(destination);
        std::filesystem::path file(objectKey);
        std::filesystem::path full_path = path / file;

        std::filesystem::path dir_path = full_path;
        dir_path.remove_filename();

        if (!std::filesystem::exists(dir_path)) {
            std::filesystem::create_directory(dir_path);
        }

        return full_path.u8string();
}

void S3Copy::worker(std::string interface, std::string bucket, std::string destination) {
    // Use a semaphore to limit the number of concurrent requests
    // And to allow us to block until all async tasks finished before exiting the thread
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

    // Grab objects until no more jobs
    Aws::S3Crt::S3CrtClient s3CrtClient = this->s3ClientFactory(interface);
    std::string key;
    while ((key = this->getJob()) != "") {
        maxConcurrent.WaitOne();
        Aws::S3Crt::Model::GetObjectRequest getRequest;
        getRequest.SetBucket(bucket);
        getRequest.SetDataReceivedEventHandler([&](const Aws::Http::HttpRequest*, Aws::Http::HttpResponse*, long long bytes) {
            this->bytesDownloaded += bytes;
        });
        getRequest.WithKey(key);
        if (this->isBenchmark) {
            getRequest.SetResponseStreamFactory([]() {
                return Aws::New<ChecksumStream>(ALLOCATION_TAG);
            });
        } else {
            auto filepath = this->prepareFilepath(destination, key);
            getRequest.SetResponseStreamFactory([filepath]() {
                return Aws::New<Aws::FStream>(
                    ALLOCATION_TAG, filepath, std::ios_base::out); 
            });
        }

        s3CrtClient.GetObjectAsync(getRequest, GetHandler, nullptr);
    }

    // Block until all jobs are complete
    // Will continually try and grab the semaphore up we've captured
    // the max allowable
    int count = this->concurrentDownloads;
    while (count != 0) {
        maxConcurrent.WaitOne();
        count -= 1;
    }
}

// Threadsafe & informs when there's nothing left to queue up
// NOTE: This will return True when our workers are still processing their last files
bool S3Copy::IsDoneQueuing() {
    return this->doneQueuingJobs && (this->objectsDownloaded == this->objectsQueued);
}

// Generates a new s3 CRT client
Aws::S3Crt::S3CrtClient S3Copy::s3ClientFactory(std::string interface) {
    Aws::S3Crt::ClientConfiguration config;
    config.region = this->region;
    config.partSize = this->partSize;
    config.scheme = this->https ? Aws::Http::Scheme::HTTPS : Aws::Http::Scheme::HTTP;
    config.throughputTargetGbps = this->throughputTargetGbps;
    config.interface = interface;
    return Aws::S3Crt::S3CrtClient(config);
}