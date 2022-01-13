#include "s3copy.h"
#include <unistd.h>

void S3Copy::Start(std::string bucket, std::string prefix, std::string destination) {
    Aws::SDKOptions options;
    // options.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Info;

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
    for (int i = 0; i < this->concurrentDownloads; i++) {
        workers[i] = new std::thread(&S3Copy::worker, this);
    }
    
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

void S3Copy::worker() {
    std::cout << "Worker starting!" << std::endl;
    std::string job;
    while ((job = this->getJob()) != "") {
        // TODO(boocolin): Implement s3 downloading
        std::cout << "Got a job!: " << job << std::endl;
    }
}
bool S3Copy::IsDone() {
    const std::lock_guard<std::mutex> lock(this->jobsMutex);
    return this->doneQueuingJobs.load() && this->jobs.size() == 0;
}