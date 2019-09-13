
#include <wdt/movers/FileS3.h>
#include <wdt/movers/FileS3Thread.h>
#include <wdt/Throttler.h>

#include <folly/lang/Bits.h>
#include <folly/hash/Checksum.h>
#include <folly/Conv.h>
#include <folly/Memory.h>
#include <folly/ScopeGuard.h>
#include <folly/String.h>

namespace facebook {
namespace wdt {


void FileS3::endCurTransfer() {
  endTime_ = Clock::now();
  WLOG(INFO) << "Last thread finished "
             << durationSeconds(endTime_ - startTime_) << " for transfer id "
             << getTransferId();
  setTransferStatus(FINISHED);
  if (throttler_) {
    throttler_->endTransfer();
  }
}

void FileS3::startNewTransfer() {
  if (throttler_) {
    throttler_->startTransfer();
  }
  WLOG(INFO) << "Starting a new transfer " << getTransferId() << " to "
             << transferRequest_.hostName;
}

FileS3::FileS3(const WdtTransferRequest &transferRequest)
    : queueAbortChecker_(this) {
  WLOG(INFO) << "FileS3: " << transferRequest.destination;
  transferRequest_ = transferRequest;

  progressReportIntervalMillis_ = options_.progress_report_interval_millis;

  /* Dont think i need
  if (getTransferId().empty()) {
    WLOG(WARNING) << "FileS3 without transferId... will likely fail to connect";
  }
  */
}

FileS3::FileS3(int port, int numSockets, const std::string &destDir)
    : FileS3(WdtTransferRequest(port, numSockets, destDir)) {
}

FileS3::~FileS3() {
  TransferStatus status = getTransferStatus();
  if (status == ONGOING) {
    WLOG(WARNING) << "FileS3 being deleted. Forcefully aborting the transfer";
    abort(ABORTED_BY_APPLICATION);
  }
  finish();
}

ErrorCode FileS3::start() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (transferStatus_ != NOT_STARTED) {
      WLOG(ERROR) << "duplicate start() call detected " << transferStatus_;
      return ALREADY_EXISTS;
    }
    transferStatus_ = ONGOING;
  }

  if( options_.buffer_size != (options_.block_size_mbytes*1024*1024)){
      WLOG(ERROR) << "S3 requires that the buffer size equals the block size.";
      return INCOMPATABLE_OPTIONS;
  }

  checkAndUpdateBufferSize();
  const bool twoPhases = options_.two_phases;
  WLOG(INFO) << "Client (sending) to " << getDestination() << ", Using ports [ "
             << transferRequest_.ports << "]";
  startTime_ = Clock::now();
  downloadResumptionEnabled_ = (transferRequest_.downloadResumptionEnabled ||
                                options_.enable_download_resumption);
  bool deleteExtraFiles = (transferRequest_.downloadResumptionEnabled ||
                           options_.delete_extra_files);
  if (!progressReporter_) {
    WVLOG(1) << "No progress reporter provided, making a default one";
    progressReporter_ = std::make_shared<ProgressReporter>(transferRequest_);
  }
  bool progressReportEnabled =
      progressReporter_ && progressReportIntervalMillis_ > 0;
  if (throttler_) {
    WLOG(INFO) << "Skipping throttler setup. External throttler set."
               << "Throttler details : " << *throttler_;
  } else {
    configureThrottler();
  }
  threadsController_ = new ThreadsController(transferRequest_.ports.size());
  threadsController_->setNumBarriers(FileS3Thread::NUM_BARRIERS);
  threadsController_->setNumFunnels(FileS3Thread::NUM_FUNNELS);
  threadsController_->setNumConditions(FileS3Thread::NUM_CONDITIONS);

  // TODO: fix this ! use transferRequest! (and dup from Receiver)
  workerThreads_ = threadsController_->makeThreads<FileS3, FileS3Thread>(
      this, transferRequest_.ports.size(), transferRequest_.ports);

  if (downloadResumptionEnabled_ && deleteExtraFiles) {
    dirQueue_->enableFileDeletion();
  }
  dirThread_ = dirQueue_->buildQueueAsynchronously();
  if (twoPhases) {
    dirThread_.join();
  }
  for (auto &senderThread : workerThreads_) {
    senderThread->startThread();
  }
  if (progressReportEnabled) {
    progressReporter_->start();
    std::thread reporterThread(&FileS3::reportProgress, this);
    progressReporterThread_ = std::move(reporterThread);
  }
  return OK;
}

bool FileS3::hasNewTransferStarted() const {
  return hasNewTransferStarted_.load();
}

const WdtTransferRequest &FileS3::init() {

  awsClientConfig_.region = options_.awsRegion;
  awsClientConfig_.scheme = Aws::Http::Scheme::HTTP;
  awsClientConfig_.endpointOverride = options_.awsEndpointOverride;
  awsClientConfig_.verifySSL = options_.awsVerifySSL;
  awsClientConfig_.maxConnections = options_.awsMaxConnections;
  awsClientConfig_.requestTimeoutMs = 5 * 60 * 1000;
  awsClientConfig_.connectTimeoutMs = 30 * 1000;

  Aws::Auth::AWSCredentials awsClientCreds_(
    options_.awsAccessKeyId,
    options_.awsSecretAccessKey
  );
  WLOG(INFO) << "auth created";
  WLOG(INFO) << options_.awsAccessKeyId;
  WLOG(INFO) << options_.awsSecretAccessKey;

  WLOG(INFO) << awsClientConfig_.endpointOverride;
  s3_client_ = Aws::S3::S3Client(awsClientCreds_,
                               awsClientConfig_,
                               Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
                               false);
  WLOG(INFO) << "CONNECTED";

  // set up directory queue
  dirQueue_.reset(new DirectorySourceQueue(options_, transferRequest_.directory,
                                           &queueAbortChecker_));
  WLOG(INFO) << "Configuring the  directory queue";
  dirQueue_->setIncludePattern(options_.include_regex);
  dirQueue_->setExcludePattern(options_.exclude_regex);
  dirQueue_->setPruneDirPattern(options_.prune_dir_regex);
  dirQueue_->setFollowSymlinks(options_.follow_symlinks);
  dirQueue_->setBlockSizeMbytes(options_.block_size_mbytes);
  dirQueue_->setNumClientThreads(transferRequest_.ports.size());
  dirQueue_->setOpenFilesDuringDiscovery(options_.open_files_during_discovery);
  dirQueue_->setDirectReads(options_.odirect_reads);
  if (!transferRequest_.fileInfo.empty() ||
      transferRequest_.disableDirectoryTraversal) {
    dirQueue_->setFileInfo(transferRequest_.fileInfo);
  }

  transferHistoryController_ =
      std::make_unique<TransferHistoryController>(*dirQueue_);

  transferLogManager_ = std::make_unique<TransferLogManager>(options_, getDestination());
  checkAndUpdateBufferSize();
  backlog_ = options_.backlog;
  if (getTransferId().empty()) {
    setTransferId(WdtBase::generateTransferId());
  }

  auto numThreads = transferRequest_.ports.size();
  // This creates the destination directory (which is needed for transferLogMgr)

  transferRequest_.downloadResumptionEnabled =
      options_.enable_download_resumption;

  if (options_.enable_download_resumption) {
    WDT_CHECK(!options_.skip_writes)
        << "Can not skip transfers with download resumption turned on";
    if (options_.resume_using_dir_tree) {
      WDT_CHECK(!options_.shouldPreallocateFiles())
          << "Can not resume using directory tree if preallocation is enabled";
    }
    ErrorCode errCode = transferLogManager_->openLog();
    if (errCode != OK) {
      WLOG(ERROR) << "Failed to open transfer log " << errorCodeToStr(errCode);
      transferRequest_.errorCode = errCode;
      return transferRequest_;
    }
    ErrorCode code = transferLogManager_->parseAndMatch(
        recoveryId_, getTransferConfig(), fileChunksInfo_);
    if (code == OK && options_.resume_using_dir_tree) {
      WDT_CHECK(fileChunksInfo_.empty());
      traverseDestinationDir(fileChunksInfo_);
    }
  }

  threadsController_ = new ThreadsController(numThreads);
  threadsController_->setNumFunnels(FileS3Thread::NUM_FUNNELS);
  threadsController_->setNumBarriers(FileS3Thread::NUM_BARRIERS);
  threadsController_->setNumConditions(FileS3Thread::NUM_CONDITIONS);
  // TODO: take transferRequest directly !
  workerThreads_ = threadsController_->makeThreads<FileS3, FileS3Thread>(
      this, transferRequest_.ports.size(), transferRequest_.ports);
  size_t numSuccessfulInitThreads = 0;
  for (auto &workerThread : workerThreads_) {
    ErrorCode code = workerThread->init();
    if (code == OK) {
      ++numSuccessfulInitThreads;
    }
  }
  WLOG(INFO) << "Registered " << numSuccessfulInitThreads
             << " successful threads";
  ErrorCode code = OK;
  const size_t targetSize = transferRequest_.ports.size();
  // TODO: replace with getNumPorts/thread
  if (numSuccessfulInitThreads != targetSize) {
    code = FEWER_PORTS;
    if (numSuccessfulInitThreads == 0) {
      code = ERROR;
    }
  }

  transferRequest_.ports.clear();
  for (const auto &workerThread : workerThreads_) {
    transferRequest_.ports.push_back(workerThread->getPort());
  }

  if (transferRequest_.hostName.empty()) {
    char hostName[1024];
    int ret = gethostname(hostName, sizeof(hostName));
    if (ret == 0) {
      transferRequest_.hostName.assign(hostName);
    } else {
      WPLOG(ERROR) << "Couldn't find the local host name";
      code = ERROR;
    }
  }
  transferRequest_.errorCode = code;
  return transferRequest_;

}

const std::string &FileS3::getDestination() const {
  ///FIXME
  return transferRequest_.destination;
}

TransferStats FileS3::getGlobalTransferStats() const {
  TransferStats globalStats;
  for (const auto &thread : workerThreads_) {
    globalStats += thread->getTransferStats();
  }
  return globalStats;
}

std::unique_ptr<TransferReport> FileS3::getTransferReport() {
  int64_t totalFileSize = 0;
  int64_t fileCount = 0;
  bool fileDiscoveryFinished = false;
  if (dirQueue_ != nullptr) {
    totalFileSize = dirQueue_->getTotalSize();
    fileCount = dirQueue_->getCount();
    fileDiscoveryFinished = dirQueue_->fileDiscoveryFinished();
  }
  double totalTime = durationSeconds(Clock::now() - startTime_);
  auto globalStats = getGlobalTransferStats();
  std::unique_ptr<TransferReport> transferReport =
      std::make_unique<TransferReport>(std::move(globalStats), totalTime,
                                       totalFileSize, fileCount,
                                       fileDiscoveryFinished);
  TransferStatus status = getTransferStatus();
  ErrorCode errCode = transferReport->getSummary().getErrorCode();
  if (status == NOT_STARTED && errCode == OK) {
    WLOG(INFO) << "Transfer not started, setting the error code to ERROR";
    transferReport->setErrorCode(ERROR);
  }
  return transferReport;
}

std::unique_ptr<TransferReport> FileS3::finish() {
  std::unique_lock<std::mutex> instanceLock(instanceManagementMutex_);
  WVLOG(1) << "FileS3::finish()";
  TransferStatus status = getTransferStatus();
  if (status == NOT_STARTED) {
    WLOG(WARNING) << "Even though transfer has not started, finish is called";
    // getTransferReport will set the error code to ERROR
    return getTransferReport();
  }
  if (status == THREADS_JOINED) {
    WVLOG(1) << "Threads have already been joined. Returning the"
             << " existing transfer report";
    return getTransferReport();
  }
  const bool twoPhases = options_.two_phases;
  bool progressReportEnabled =
      progressReporter_ && progressReportIntervalMillis_ > 0;
  for (auto &senderThread : workerThreads_) {
    senderThread->finish();
  }
  if (!twoPhases) {
    dirThread_.join();
  }
  WDT_CHECK(numActiveThreads_ == 0);
  setTransferStatus(THREADS_JOINED);
  if (progressReportEnabled) {
    progressReporterThread_.join();
  }
  std::vector<TransferStats> threadStats;
  for (auto &senderThread : workerThreads_) {
    threadStats.push_back(senderThread->moveStats());
  }

  bool allSourcesAcked = false;
  for (auto &senderThread : workerThreads_) {
    auto &stats = senderThread->getTransferStats();
    if (stats.getErrorCode() == OK) {
      // at least one thread finished correctly
      // that means all transferred sources are acked
      allSourcesAcked = true;
      break;
    }
  }

  std::vector<TransferStats> transferredSourceStats;
  for (auto port : transferRequest_.ports) {
    auto &transferHistory =
        transferHistoryController_->getTransferHistory(port);
    if (allSourcesAcked) {
      transferHistory.markAllAcknowledged();
    } else {
      transferHistory.returnUnackedSourcesToQueue();
    }
    if (options_.full_reporting) {
      std::vector<TransferStats> stats = transferHistory.popAckedSourceStats();
      transferredSourceStats.insert(transferredSourceStats.end(),
                                    std::make_move_iterator(stats.begin()),
                                    std::make_move_iterator(stats.end()));
    }
  }
  if (options_.full_reporting) {
    validateTransferStats(transferredSourceStats,
                          dirQueue_->getFailedSourceStats());
  }
  int64_t totalFileSize = dirQueue_->getTotalSize();
  double totalTime = durationSeconds(endTime_ - startTime_);
  std::unique_ptr<TransferReport> transferReport =
      std::make_unique<TransferReport>(
          transferredSourceStats, dirQueue_->getFailedSourceStats(),
          threadStats, dirQueue_->getFailedDirectories(), totalTime,
          totalFileSize, dirQueue_->getCount(),
          dirQueue_->getPreviouslySentBytes(),
          dirQueue_->fileDiscoveryFinished());

  if (progressReportEnabled) {
    progressReporter_->end(transferReport);
  }
  logPerfStats();

  double directoryTime;
  directoryTime = dirQueue_->getDirectoryTime();
  WLOG(INFO) << "Total sender time = " << totalTime << " seconds ("
             << directoryTime << " dirTime)"
             << ". Transfer summary : " << *transferReport << "\n"
             << WDT_LOG_PREFIX << "Total sender throughput = "
             << transferReport->getThroughputMBps() << " Mbytes/sec ("
             << transferReport->getSummary().getEffectiveTotalBytes() /
                    (totalTime - directoryTime) / kMbToB
             << " Mbytes/sec pure transfer rate)";
  return transferReport;
}

ErrorCode FileS3::transferAsync() {
  return start();
}

std::unique_ptr<TransferReport> FileS3::transfer() {
  start();
  return finish();
}

ErrorCode FileS3::validateTransferRequest() {
  ErrorCode code = WdtBase::validateTransferRequest();
  // If the request is still valid check for other
  // sender specific validations
  if (code == OK && transferRequest_.hostName.empty()) {
    WLOG(ERROR) << "Transfer request validation failed for file file "
                << transferRequest_.getLogSafeString();
    code = INVALID_REQUEST;
  }
  transferRequest_.errorCode = code;
  return code;
}

void FileS3::validateTransferStats(
    const std::vector<TransferStats> &transferredSourceStats,
    const std::vector<TransferStats> &failedSourceStats) {
  int64_t sourceFailedAttempts = 0;
  int64_t sourceDataBytes = 0;
  int64_t sourceEffectiveDataBytes = 0;
  int64_t sourceNumBlocks = 0;

  int64_t threadFailedAttempts = 0;
  int64_t threadDataBytes = 0;
  int64_t threadEffectiveDataBytes = 0;
  int64_t threadNumBlocks = 0;

  for (const auto &stat : transferredSourceStats) {
    sourceFailedAttempts += stat.getFailedAttempts();
    sourceDataBytes += stat.getDataBytes();
    sourceEffectiveDataBytes += stat.getEffectiveDataBytes();
    sourceNumBlocks += stat.getNumBlocks();
  }
  for (const auto &stat : failedSourceStats) {
    sourceFailedAttempts += stat.getFailedAttempts();
    sourceDataBytes += stat.getDataBytes();
    sourceEffectiveDataBytes += stat.getEffectiveDataBytes();
    sourceNumBlocks += stat.getNumBlocks();
  }
  for (const auto &senderThread : workerThreads_) {
    const auto &stat = senderThread->getTransferStats();
    threadFailedAttempts += stat.getFailedAttempts();
    threadDataBytes += stat.getDataBytes();
    threadEffectiveDataBytes += stat.getEffectiveDataBytes();
    threadNumBlocks += stat.getNumBlocks();
  }

  WDT_CHECK(sourceFailedAttempts == threadFailedAttempts);
  WDT_CHECK(sourceDataBytes == threadDataBytes);
  WDT_CHECK(sourceEffectiveDataBytes == threadEffectiveDataBytes);
  WDT_CHECK(sourceNumBlocks == threadNumBlocks);
}

void FileS3::reportProgress() {
  WDT_CHECK(progressReportIntervalMillis_ > 0);
  int throughputUpdateIntervalMillis =
      options_.throughput_update_interval_millis;
  WDT_CHECK(throughputUpdateIntervalMillis >= 0);
  int throughputUpdateInterval =
      throughputUpdateIntervalMillis / progressReportIntervalMillis_;

  int64_t lastEffectiveBytes = 0;
  std::chrono::time_point<Clock> lastUpdateTime = Clock::now();
  int intervalsSinceLastUpdate = 0;
  double currentThroughput = 0;

  auto waitingTime = std::chrono::milliseconds(progressReportIntervalMillis_);
  WLOG(INFO) << "Progress reporter tracking every "
             << progressReportIntervalMillis_ << " ms";
  while (true) {
    {
      std::unique_lock<std::mutex> lock(mutex_);
      conditionFinished_.wait_for(lock, waitingTime);
      if (transferStatus_ == THREADS_JOINED) {
        break;
      }
    }

    std::unique_ptr<TransferReport> transferReport = getTransferReport();
    intervalsSinceLastUpdate++;
    if (intervalsSinceLastUpdate >= throughputUpdateInterval) {
      auto curTime = Clock::now();
      int64_t curEffectiveBytes =
          transferReport->getSummary().getEffectiveDataBytes();
      double time = durationSeconds(curTime - lastUpdateTime);
      currentThroughput = (curEffectiveBytes - lastEffectiveBytes) / time;
      lastEffectiveBytes = curEffectiveBytes;
      lastUpdateTime = curTime;
      intervalsSinceLastUpdate = 0;
    }
    transferReport->setCurrentThroughput(currentThroughput);

    progressReporter_->progress(transferReport);
    if (reportPerfSignal_.notified()) {
      logPerfStats();
    }
  }
}

void FileS3::logPerfStats() const {
  if (!options_.enable_perf_stat_collection) {
    return;
  }

  PerfStatReport report(options_);
  for (auto &senderThread : workerThreads_) {
    report += senderThread->getPerfReport();
  }
  report += dirQueue_->getPerfReport();
  WLOG(INFO) << report;
}

void FileS3::endCurGlobalSession() {
  setTransferStatus(FINISHED);
  if (!hasNewTransferStarted_) {
    WLOG(WARNING) << "WDT transfer did not start, no need to end session";
    return;
  }
  WLOG(INFO) << "Ending the transfer " << getTransferId();
  if (throttler_) {
    throttler_->endTransfer();
  }
  checkpoints_.clear();
  // TODO might consider moving closing the transfer log here
  hasNewTransferStarted_.store(false);
}

void FileS3::addCheckpoint(Checkpoint checkpoint) {
  WLOG(INFO) << "Adding global checkpoint " << checkpoint.port << " "
             << checkpoint.numBlocks << " "
             << checkpoint.lastBlockReceivedBytes;
  checkpoints_.emplace_back(checkpoint);
}

void FileS3::setRecoveryId(const std::string &recoveryId) {
  recoveryId_ = recoveryId;
  WLOG(INFO) << "recovery id " << recoveryId_;
}

int64_t FileS3::getTransferConfig() const {
  int64_t config = 0;
  if (options_.resume_using_dir_tree) {
    config |= (1 << 1);
  }
  return config;
}

void FileS3::traverseDestinationDir(
    std::vector<FileChunksInfo> &fileChunksInfo) {
  DirectorySourceQueue dirQueue(options_, getDirectory(),
                                &abortCheckerCallback_);
  dirQueue.buildQueueSynchronously();
  auto &discoveredFilesInfo = dirQueue.getDiscoveredFilesMetaData();
  for (auto &fileInfo : discoveredFilesInfo) {
    if (fileInfo->relPath == kWdtLogName ||
        fileInfo->relPath == kWdtBuggyLogName) {
      // do not include wdt log files
      WVLOG(1) << "Removing " << fileInfo->relPath
               << " from the list of existing files";
      continue;
    }
    FileChunksInfo chunkInfo(fileInfo->seqId, fileInfo->relPath,
                             fileInfo->size);
    chunkInfo.addChunk(Interval(0, fileInfo->size));
    fileChunksInfo.emplace_back(std::move(chunkInfo));
  }
  return;
}

}
}  // namespace facebook::wdt
