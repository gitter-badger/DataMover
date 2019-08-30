
#include <wdt/workers/FileFile.h>
#include <wdt/workers/FileFileThread.h>
#include <wdt/Throttler.h>

#include <folly/lang/Bits.h>
#include <folly/hash/Checksum.h>
#include <folly/Conv.h>
#include <folly/Memory.h>
#include <folly/ScopeGuard.h>
#include <folly/String.h>

namespace facebook {
namespace wdt {

FileFile::FileFile(const WdtTransferRequest &transferRequest)
    : queueAbortChecker_(this) {
  WLOG(INFO) << "FileFile " << Protocol::getFullVersion();
  transferRequest_ = transferRequest;

  progressReportIntervalMillis_ = options_.progress_report_interval_millis;

  /* Dont think i need
  if (getTransferId().empty()) {
    WLOG(WARNING) << "FileFile without transferId... will likely fail to connect";
  }
  */
}

FileFile::FileFile(int port, int numSockets, const std::string &destDir)
    : FileFile(WdtTransferRequest(port, numSockets, destDir)) {
}

FileFile::~FileFile() {
  TransferStatus status = getTransferStatus();
  if (status == ONGOING) {
    WLOG(WARNING) << "FileFile being deleted. Forcefully aborting the transfer";
    abort(ABORTED_BY_APPLICATION);
  }
  finish();
}

ErrorCode FileFile::start() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (transferStatus_ != NOT_STARTED) {
      WLOG(ERROR) << "duplicate start() call detected " << transferStatus_;
      return ALREADY_EXISTS;
    }
    transferStatus_ = ONGOING;
  }

  // set up directory queue
  dirQueue_.reset(new DirectorySourceQueue(options_, transferRequest_.directory,
                                           &queueAbortChecker_));
  WVLOG(3) << "Configuring the  directory queue";
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
  threadsController_->setNumBarriers(FileFileThread::NUM_BARRIERS);
  threadsController_->setNumFunnels(FileFileThread::NUM_FUNNELS);
  threadsController_->setNumConditions(FileFileThread::NUM_CONDITIONS);
  // TODO: fix this ! use transferRequest! (and dup from Receiver)
  workerThreads_ = threadsController_->makeThreads<FileFile, FileFileThread>(
      this, transferRequest_.ports.size(), transferRequest_.ports);
  if (downloadResumptionEnabled_ && deleteExtraFiles) {
    if (getProtocolVersion() >= Protocol::DELETE_CMD_VERSION) {
      dirQueue_->enableFileDeletion();
    } else {
      WLOG(WARNING) << "Turning off extra file deletion on the receiver side "
                       "because of protocol version "
                    << getProtocolVersion();
    }
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
    std::thread reporterThread(&FileFile::reportProgress, this);
    progressReporterThread_ = std::move(reporterThread);
  }
  return OK;
}

const std::string &FileFile::getDestination() const {
  return transferRequest_.destDir;
}

TransferStats FileFile::getGlobalTransferStats() const {
  TransferStats globalStats;
  for (const auto &thread : workerThreads_) {
    globalStats += thread->getTransferStats();
  }
  return globalStats;
}

TransferStats FileFile::getGlobalTransferStats() const {
  TransferStats globalStats;
  for (const auto &thread : workerThreads_) {
    globalStats += thread->getTransferStats();
  }
  return globalStats;
}

std::unique_ptr<TransferReport> FileFile::finish() {
  std::unique_lock<std::mutex> instanceLock(instanceManagementMutex_);
  WVLOG(1) << "FileFile::finish()";
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

ErrorCode FileFile::transferAsync() {
  return start();
}

std::unique_ptr<TransferReport> FileFile::transfer() {
  start();
  return finish();
}

void FileFile::validateTransferStats(
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

void FileFile::reportProgress() {
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

void FileFile::logPerfStats() const {
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

void FileFile::endCurGlobalSession() {
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
  if (fileCreator_) {
    fileCreator_->clearAllocationMap();
  }
  // TODO might consider moving closing the transfer log here
  hasNewTransferStarted_.store(false);
}

void FileFile::addCheckpoint(Checkpoint checkpoint) {
  WLOG(INFO) << "Adding global checkpoint " << checkpoint.port << " "
             << checkpoint.numBlocks << " "
             << checkpoint.lastBlockReceivedBytes;
  checkpoints_.emplace_back(checkpoint);
}

}
}  // namespace facebook::wdt
