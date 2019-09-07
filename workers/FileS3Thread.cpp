
#include <wdt/workers/FileS3Thread.h>
#include <folly/Conv.h>
#include <folly/Memory.h>
#include <folly/ScopeGuard.h>
#include <folly/String.h>
#include <folly/lang/Bits.h>
#include <wdt/util/FileWriter.h>
#include <openssl/md5.h>
#include <sys/stat.h>
#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/CreateMultipartUploadResult.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/CompleteMultipartUploadResult.h>
#include <aws/s3/model/UploadPartRequest.h>
#include <aws/s3/model/UploadPartResult.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/auth/AWSCredentialsProvider.h>


namespace facebook {
namespace wdt {

const FileS3Thread::StateFunction FileS3Thread::stateMap_[] = {
    &FileS3Thread::copyFileChunk,
    &FileS3Thread::checkForAbort,
    &FileS3Thread::finishWithError};

void FileS3Thread::start() {
  if (buf_ == nullptr) {
    WTLOG(ERROR) << "Unable to allocate buffer";
    threadStats_.setLocalErrorCode(MEMORY_ALLOCATION_ERROR);
    return;
  }

  controller_->executeAtStart([&]() { wdtParent_->startNewTransfer(); });

  FileS3State state = COPY_FILE_CHUNK;
  while (true) {
    ErrorCode abortCode = wdtParent_->getCurAbortCode();
    if (abortCode != OK) {
      WTLOG(ERROR) << "Transfer aborted " << " "
                   << errorCodeToStr(abortCode);
      threadStats_.setLocalErrorCode(ABORT);
      break;
    }
    if (state == END) {
      break;
    }
    state = (this->*stateMap_[state])();
  }
  controller_->deRegisterThread(threadIndex_);
  //controller_->executeAtEnd([&]() { wdtParent_->endCurGlobalSession(); });
  controller_->executeAtEnd([&]() { wdtParent_->endCurTransfer(); });
  //WTLOG(INFO) << threadStats_;
}

ErrorCode FileS3Thread::init() {
  return OK;
}

int32_t FileS3Thread::getPort() const {
  return port_;
}

void FileS3Thread::reset() {
  numRead_ = off_ = 0;
  //checkpointIndex_ = pendingCheckpointIndex_ = 0;
  //senderReadTimeout_ = senderWriteTimeout_ = -1;
  //curConnectionVerified_ = false;
  threadStats_.reset();
  //checkpoints_.clear();
  //newCheckpoints_.clear();
  //checkpoint_ = Checkpoint(getPort());
}

FileS3State FileS3Thread::copyFileChunk() {
  WTVLOG(1) << "entered COPY_FILE_CHUNK state";
  ThreadTransferHistory &transferHistory = getTransferHistory();

  ErrorCode transferStatus;
  std::unique_ptr<ByteSource> source =
      dirQueue_->getNextSource(threadCtx_.get(), transferStatus);

  if (!source) {
      return END;
  }
  WDT_CHECK(!source->hasError());
  TransferStats transferStats = copyOneByteSource(source, transferStatus);
  threadStats_ += transferStats;
  source->addTransferStats(transferStats);
  source->close();
  if (!transferHistory.addSource(source)) {
    // global checkpoint received for this thread. no point in
    // continuing
    WTLOG(ERROR) << "global checkpoint received. Stopping";
    threadStats_.setLocalErrorCode(CONN_ERROR);
    return END;
  }
  if (transferStats.getLocalErrorCode() != OK) {
    return FINISH_WITH_ERROR;
  }
  return COPY_FILE_CHUNK;
}

TransferStats FileS3Thread::copyOneByteSource(
    const std::unique_ptr<ByteSource> &source, ErrorCode transferStatus) {
  TransferStats stats;
  int64_t off = 0;
  off += sizeof(int16_t);
  const int64_t expectedSize = source->getSize();
  int64_t actualSize = 0;
  const SourceMetaData &metadata = source->getMetaData();
  BlockDetails blockDetails;
  blockDetails.fileName = metadata.relPath;
  blockDetails.seqId = metadata.seqId;
  blockDetails.fileSize = metadata.size;
  blockDetails.offset = source->getOffset();
  blockDetails.dataSize = expectedSize;
  blockDetails.allocationStatus = metadata.allocationStatus;
  blockDetails.prevSeqId = metadata.prevSeqId;


  WLOG(INFO) << " Read id:" << blockDetails.fileName
            << " size:" << blockDetails.dataSize << " ooff:" << oldOffset_;

  while (!source->finished()) {
    int64_t size;
    char *buffer = source->read(size);
    WLOG(INFO) << "Read size: " << size;
    if (source->hasError()) {
      WTLOG(ERROR) << "Failed reading file " << source->getIdentifier()
                   << " for fd " << metadata.fd ;
      break;
    }

    auto throttler = wdtParent_->getThrottler();
    if (throttler) {
        // We might be reading more than we require for this file but
        // throttling should make sense for any additional bytes received
        // on the network
        throttler->limit(*threadCtx_, size);
    }

    //////////////////////////////// AWS 
    Aws::SDKOptions options;
    Aws::InitAPI(options);

    const Aws::String AWS_ACCESS_KEY_ID = "cmajoros";
    const Aws::String AWS_SECRET_ACCESS_KEY = "8da993fb51ad1a4254c936f855c3a45e";
    const Aws::Auth::AWSCredentials clientCreds(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY);

    const std::shared_ptr<Aws::IOStream> request_body =
        Aws::MakeShared<Aws::StringStream>("");

// clientConfiguration = @0x7fffe5a8b890: {userAgent = "aws-sdk-cpp/1.7.176 Linux/3.10.0-957.27.2.el7.x86_64 x86_64 GCC/8.3.1", scheme = Aws::Http::HTTP, region = "us-east-1", useDualStack = false, maxConnections = 2, httpRequestTimeoutMs = 0, requestTimeoutMs = 5000, connectTimeoutMs = 1000, 
// enableTcpKeepAlive = true, tcpKeepAliveIntervalMs = 30000, lowSpeedLimit = 1, retryStrategy = std::shared_ptr (count 2, weak 0) 0x7fffe0000e80, endpointOverride = "", proxyScheme = Aws::Http::HTTP, proxyHost = "", proxyPort = 0, proxyUserName = "", proxyPassword = "", proxySSLCertPath = "", proxySSLCertType = "", 
// proxySSLKeyPath = "", proxySSLKeyType = "", proxySSLKeyPassword = "", executor = std::shared_ptr (count 1, weak 0) 0x7fffe0001ac0, verifySSL = true, caPath = "", caFile = "", writeRateLimiter = std::shared_ptr (empty) 0x0, readRateLimiter = std::shared_ptr (empty) 0x0, httpLibOverride = Aws::Http::DEFAULT_CLIENT, 
// followRedirects = true, disableExpectHeader = false, enableClockSkewAdjustment = true, enableHostPrefixInjection = true, enableEndpointDiscovery = false}


    Aws::Client::ClientConfiguration clientConfig;
    clientConfig.region = Aws::String("us-east-1");
    clientConfig.scheme = Aws::Http::Scheme::HTTP;
    //clientConfig.endpointOverride = "swift19.jfk3.bamtech.co:80";
    clientConfig.endpointOverride = Aws::String("http://swift19.jfk3.bamtech.co:80");
    clientConfig.verifySSL = false;
    clientConfig.maxConnections = 128;

    //Aws::S3::S3Client s3_client(clientConfig);
    //Aws::S3::S3Client s3_client(Aws::Auth::AWSCredentials("cmajoros", "9da993fb51ad1a4254c936f855c3a45e"), clientConfig);
    //auto s3_client = Aws::S3::S3Client(Aws::Auth::AWSCredentials("cmajoros", "9da993fb51ad1a4254c936f855c3a45e"), clientConfig, false);

    Aws::S3::S3Client s3_client(clientCreds, clientConfig, Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, false);
    *request_body << buffer;

    const Aws::String bucket = "cmajoros";
    Aws::S3::Model::CreateMultipartUploadRequest cmu_request;
    cmu_request.SetBucket(bucket); /// FIXME
    cmu_request.SetKey(blockDetails.fileName.c_str());

    Aws::S3::Model::CreateMultipartUploadOutcome cmu_response = s3_client.CreateMultipartUpload(cmu_request);

    if (!cmu_response.IsSuccess()) {
        auto error = cmu_response.GetError();
        WLOG(ERROR) << "cmu s3 ERROR: " << error.GetExceptionName() << " -- "
            << error.GetMessage();
        WLOG(INFO) << "bucketname: !" << cmu_request.GetBucket() << "!";
        threadStats_.setLocalErrorCode(ABORT);
        stats.incrFailedAttempts();
        return stats;
    }

    Aws::S3::Model::CreateMultipartUploadResult cmu_result;
    cmu_result = cmu_response.GetResult();
    WLOG(INFO) << "Upload ID: " << cmu_result.GetUploadId();

    WLOG(INFO) << "Block num: " << source->getBlockNum();
    Aws::S3::Model::UploadPartRequest request;
    request.SetBucket(bucket);
    request.SetKey(blockDetails.fileName.c_str());
    request.SetUploadId(cmu_result.GetUploadId());
    request.SetPartNumber(source->getBlockNum());
    request.SetBody(request_body);

    Aws::S3::Model::UploadPartOutcome response = s3_client.UploadPart(request);
    stats.addDataBytes(size);

    if (!response.IsSuccess()) {
        auto error = response.GetError();
        WLOG(ERROR) << "s3 ERROR: " << error.GetExceptionName() << " -- "
            << error.GetMessage();
        WLOG(INFO) << "bucketname: !" << request.GetBucket() << "!";
        threadStats_.setLocalErrorCode(ABORT);
        stats.incrFailedAttempts();
        // virtual Model::AbortMultipartUploadOutcome Aws::S3::S3Client::AbortMultipartUpload   (   const Model::AbortMultipartUploadRequest &     request )   const
        return stats;
    }

    Aws::S3::Model::UploadPartResult result;
    result = response.GetResult();
    WLOG(INFO) << "File Name: " << blockDetails.fileName << " MD5: " << result.GetETag();

    Aws::S3::Model::CompletedMultipartUpload completed;
    Aws::S3::Model::CompletedPart completedPart;
    completedPart.SetPartNumber(source->getBlockNum());
    completedPart.SetETag(result.GetETag());
    Aws::Vector<Aws::S3::Model::CompletedPart> completed_parts = {completedPart};
    completed.SetParts(completed_parts);

    Aws::S3::Model::CompleteMultipartUploadRequest complete_request;
    complete_request.SetBucket(bucket);
    complete_request.SetKey(blockDetails.fileName.c_str());
    complete_request.SetUploadId(cmu_result.GetUploadId());
    complete_request.SetMultipartUpload(completed);

    Aws::S3::Model::CompleteMultipartUploadOutcome complete_response = s3_client.CompleteMultipartUpload(complete_request);
    if (!complete_response.IsSuccess()) {
        auto error = complete_response.GetError();
        WLOG(ERROR) << "complete s3 ERROR: " << error.GetExceptionName() << " -- "
            << error.GetMessage();
        WLOG(INFO) << "bucketname: !" << request.GetBucket() << "!";
        threadStats_.setLocalErrorCode(ABORT);
        stats.incrFailedAttempts();
        // virtual Model::AbortMultipartUploadOutcome Aws::S3::S3Client::AbortMultipartUpload   (   const Model::AbortMultipartUploadRequest &     request )   const
        return stats;
    }
    WLOG(INFO) << "File Name: " << blockDetails.fileName << " MD5: " << result.GetETag();

    WLOG(INFO) << "WOOT!!!!";
    // TODO: actually validate the etag
    //
    //////////////////////////////// AWS 

    if (wdtParent_->getCurAbortCode() != OK) {
        WTLOG(ERROR) << "Thread marked for abort while processing "
                    << blockDetails.fileName << " " << blockDetails.seqId;
        threadStats_.setLocalErrorCode(ABORT);
        return stats;
    }

    actualSize += size;
  }

  if (getThreadAbortCode() != OK) {
    WTLOG(ERROR) << "Transfer aborted during block transfer "
                 << source->getIdentifier();
    stats.setLocalErrorCode(ABORT);
    stats.incrFailedAttempts();
    return stats;
  }

  WVLOG(2) << "completed " << blockDetails.fileName;
  stats.setLocalErrorCode(OK);
  stats.incrNumBlocks();
  stats.addEffectiveBytes(0, stats.getDataBytes());
  return stats;
};

FileS3State FileS3Thread::checkForAbort() {
  WTLOG(INFO) << "entered CHECK_FOR_ABORT state";
  return COPY_FILE_CHUNK;
}

FileS3State FileS3Thread::finishWithError() {
  WTLOG(INFO) << "entered FINISH_WITH_ERROR state";
  // should only be in this state if there is some error
  WDT_CHECK(threadStats_.getLocalErrorCode() != OK);

  auto cv = controller_->getCondition(0);//FIXME
  auto guard = cv->acquire();
  wdtParent_->addCheckpoint(checkpoint_);
  controller_->markState(threadIndex_, FINISHED);
  guard.notifyOne();
  return END;
}

std::ostream &operator<<(std::ostream &os, const FileS3Thread &workerThread) {
  os << "Thread[" << workerThread.threadIndex_
     << ", port: " << workerThread.port_ << "] ";
  return os;
}

ErrorCode FileS3Thread::getThreadAbortCode() {
  ErrorCode globalAbortCode = wdtParent_->getCurAbortCode();
  if (globalAbortCode != OK) {
    return globalAbortCode;
  }
  if (getTransferHistory().isGlobalCheckpointReceived()) {
    return GLOBAL_CHECKPOINT_ABORT;
  }
  return OK;
}

}
}

