#include "s3copy.h"
#include <fstream>
#include <unistd.h>
#include <aws/s3-crt/model/ListObjectsV2Request.h>
#include <aws/s3-crt/model/GetObjectRequest.h>
#include <aws/core/Aws.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>
#include <aws/core/utils/logging/CRTLogSystem.h>
#include <thread>
#include <filesystem>
#include <chrono>

void S3Copy::Start(std::string bucket, std::string prefix, std::string destination) {
    Aws::SDKOptions options;
    options.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Info;
    options.ioOptions.clientBootstrap_create_fn = []() {
        Aws::Crt::Io::EventLoopGroup eventLoopGroup(0 /* cpuGroup */, 32 /* threadCount */);
        Aws::Crt::Io::DefaultHostResolver defaultHostResolver(eventLoopGroup, 16 /* maxHosts */, 300 /* maxTTL */);
        auto clientBootstrap = Aws::MakeShared<Aws::Crt::Io::ClientBootstrap>("aws-crt", eventLoopGroup, defaultHostResolver);
        clientBootstrap->EnableBlockingShutdown();
        return clientBootstrap;
    };



    Aws::S3Crt::ClientConfiguration config;
    config.region = this->region;
    config.throughputTargetGbps = this->throughputTargetGbps;
    config.partSize = this->partSize;

    // Code Block forces our s3 crt client to be freed up once we're done using it
    // https://docs.aws.amazon.com/sdk-for-cpp/v1/developer-guide/basic-use.html
    Aws::InitAPI(options);
    
    this->s3CrtClient = new Aws::S3Crt::S3CrtClient(config);

    // Start thread to get S3 objects in bucket & prefix
    std::thread queueThread(&S3Copy::queueObjects, this, bucket, prefix);

    // Start up pool of workers to have concurrent downloads
    std::thread **workers = new std::thread*[this->concurrentDownloads];
    Aws::S3Crt::S3CrtClient **srtClients = new Aws::S3Crt::S3CrtClient*[this->concurrentDownloads];
    for (int i = 0; i < this->concurrentDownloads; i++) {
        srtClients[i] = new Aws::S3Crt::S3CrtClient(config);
        workers[i] = new std::thread(&S3Copy::worker, this, srtClients[i], bucket, destination);
    }
    

    // Output throughput
    auto start = std::chrono::system_clock::now();
    float progress = 0.0;
    int barWidth = 70;
    while (!this->IsDone()) {
        progress =  (double) this->bytesDownloaded / this->bytesQueued;
        
        std::cout << "[";
        int pos = barWidth * progress;
        for (int i = 0; i < barWidth; ++i) {
            if (i < pos) std::cout << "=";
            else if (i == pos) std::cout << ">";
            else std::cout << " ";
        }

        auto dur = std::chrono::system_clock::now() - start;
        auto secs = std::chrono::duration<double>(dur).count();

        double gibps = ((double) this->bytesDownloaded / 1024 / 1024 / 1024) * 8 / secs;
        std::cout << std::fixed;
        std::cout << std::setprecision(2);
        std::cout << "] " << int(progress * 100.0) << "% "
            << " [" << (double) this->bytesDownloaded / 1024 /1024 / 1024 << "" 
            << "/" << (double) this->bytesQueued / 1024 / 1024 / 1024 << "GiB] " 
            << "[" << gibps << " Gibps]"
            << " [" << this->jobs.size() << " objects remaining]"
            << "\r";
        std::cout.flush();
        sleep(1);
    }

    //     std::chrono::duration<double> diff = end - start;
    //     std::cout << "Time to download: " << (this->bytesDownloaded / 1024 / 1024) << "MiB "
    //               "took " << diff.count() << std::endl;
    //     sleep(3);
    // }


    // Wait for threads to finish
    queueThread.join();
    for (int i = 0; i < this->concurrentDownloads; i++) {
        workers[i]->join();
    }

    // Clean up s3 client;
    delete this->s3CrtClient;

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

void S3Copy::worker(Aws::S3Crt::S3CrtClient *client, std::string bucket, std::string destination) {
    std::string job;
    while ((job = this->getJob()) != "") {
        Aws::S3Crt::Model::GetObjectRequest request;
        request.SetBucket(bucket);
        request.SetKey(job);
        Aws::S3Crt::Model::GetObjectOutcome outcome = client->GetObject(request);
        if (!outcome.IsSuccess()) {
            std::cerr << "Error downloading object: s3://" << bucket << "/" << job << " " << outcome.GetError().GetMessage() << std::endl;
            continue;
        }

        std::string filename = "/tmp/tt/" + job;
        std::filesystem::path path(destination);
        std::filesystem::path file(job);
        std::filesystem::path full_path = path / file;
        std::filesystem::path dir_path = full_path;
        dir_path.remove_filename();

        // std::cout << "Saving to: " << full_path << std::endl;
        if (!std::filesystem::exists(dir_path)) {
            std::filesystem::create_directory(dir_path);
        }

        size_t bytes = outcome.GetResult().GetContentLength();
        
        std::ofstream output_file(full_path.c_str(), std::ios::out |std::ios::binary);
        output_file << outcome.GetResult().GetBody().rdbuf();
        this->bytesDownloaded += bytes;
    }
}
bool S3Copy::IsDone() {
    const std::lock_guard<std::mutex> lock(this->jobsMutex);
    return this->doneQueuingJobs.load() && this->jobs.size() == 0;
}