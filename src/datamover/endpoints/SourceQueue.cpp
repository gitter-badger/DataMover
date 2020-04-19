
#include <datamover/endpoints/SourceQueue.h>
#include "SourceQueue.h"

namespace datamover {

bool SourceQueue::finished() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return initFinished_ && sourceQueue_.empty();
}

int64_t SourceQueue::getCount() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return numEntries_;
}

const PerfStatReport &SourceQueue::getPerfReport() const {
  return threadCtx_->getPerfReport();
}

bool datamover::SourceQueue::fileDiscoveryFinished() {
  return false;
}

bool datamover::SourceQueue::setRootDir(const std::string &newRootDir) {
  return false;
}

void SourceQueue::setIncludePattern(const std::string &includePattern) {
  includePattern_ = includePattern;
}

void SourceQueue::setExcludePattern(const std::string &excludePattern) {
  excludePattern_ = excludePattern;
}

void SourceQueue::setPruneDirPattern(const std::string &pruneDirPattern) {
  pruneDirPattern_ = pruneDirPattern;
}

void SourceQueue::setBlockSizeMbytes(int64_t blockSizeMbytes) {
  blockSizeMbytes_ = blockSizeMbytes;
}

void SourceQueue::createIntoQueue(const std::string &fullPath,
                                  WdtFileInfo &fileInfo) {
}

void SourceQueue::createIntoQueueInternal(SourceMetaData *metadata) {
}

void SourceQueue::setFileInfo(
    const std::vector<WdtFileInfo> &fileInfo) {
  fileInfo_ = fileInfo;
  exploreDirectory_ = false;
}

const std::vector<WdtFileInfo> &SourceQueue::getFileInfo() const {
  return fileInfo_;
}

std::vector<SourceMetaData *> &SourceQueue::getDiscoveredFilesMetaData() {
  return sharedFileData_;
}

std::pair<int64_t, ErrorCode> SourceQueue::getNumBlocksAndStatus() const {
  std::lock_guard<std::mutex> lock(mutex_);
  ErrorCode status = OK;
  if (!failedSourceStats_.empty() || !failedDirectories_.empty()) {
    // this function is called by active sender threads. The only way files or
    // directories can fail when sender threads are active is due to read errors
    status = BYTE_SOURCE_READ_ERROR;
  }
  return std::make_pair(numBlocks_, status);
}

int64_t SourceQueue::getTotalSize() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return totalFileSize_;
}

int64_t SourceQueue::getPreviouslySentBytes() const {
  return previouslySentBytes_;
}

bool SourceQueue::fileDiscoveryFinished() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return initFinished_;
}

void SourceQueue::clearSourceQueue() {
  // clear current content of the queue. For some reason, priority_queue does
  // not have a clear method
  while (!sourceQueue_.empty()) {
    sourceQueue_.pop();
  }
}

bool SourceQueue::buildQueueSynchronously() {
  return true;
}

void SourceQueue::returnToQueue(
    std::vector<std::unique_ptr<ByteSource>> &sources) {
  int returnedCount = 0;
  std::unique_lock<std::mutex> lock(mutex_);
  for (auto &source : sources) {
    sourceQueue_.push(std::move(source));
    returnedCount++;
    WDT_CHECK_GT(numBlocksDequeued_, 0);
    numBlocksDequeued_--;
  }
  lock.unlock();
  smartNotify(returnedCount);
}

void SourceQueue::returnToQueue(std::unique_ptr<ByteSource> &source) {
  std::vector<std::unique_ptr<ByteSource>> sources;
  sources.emplace_back(std::move(source));
  returnToQueue(sources);
}

void SourceQueue::smartNotify(int32_t addedSource) {
  if (addedSource >= numClientThreads_) {
    conditionNotEmpty_.notify_all();
    return;
  }
  for (int i = 0; i < addedSource; i++) {
    conditionNotEmpty_.notify_one();
  }
}
void setPreviouslyReceivedChunks(
    std::vector<FileChunksInfo> &previouslyTransferredChunks) {
  std::unique_lock<std::mutex> lock(mutex_);
  WDT_CHECK_EQ(0, numBlocksDequeued_);
  // reset all the queue variables
  nextSeqId_ = 0;
  totalFileSize_ = 0;
  numEntries_ = 0;
  numBlocks_ = 0;
  for (auto &chunkInfo : previouslyTransferredChunks) {
    nextSeqId_ = std::max(nextSeqId_, chunkInfo.getSeqId() + 1);
    auto fileName = chunkInfo.getFileName();
    previouslyTransferredChunks_.insert(
        std::make_pair(std::move(fileName), std::move(chunkInfo)));
  }
  clearSourceQueue();
  // recreate the queue
  for (const auto metadata : sharedFileData_) {
    // TODO: do not notify inside createIntoQueueInternal. This method still
    // holds the lock, so no point in notifying
    createIntoQueueInternal(metadata);
  }
  enqueueFilesToBeDeleted();
}

std::thread SourceQueue::buildQueueAsynchronously() {
  // relying on RVO (and thread not copyable to avoid multiple ones)
  return std::thread(&SourceQueue::buildQueueSynchronously, this);
}

bool SourceQueue::explore() {
  return true;
}

void SourceQueue::setPreviouslyReceivedChunks(
    std::vector<FileChunksInfo> &previouslyTransferredChunks) {
  std::unique_lock<std::mutex> lock(mutex_);
  WDT_CHECK_EQ(0, numBlocksDequeued_);
  // reset all the queue variables
  nextSeqId_ = 0;
  totalFileSize_ = 0;
  numEntries_ = 0;
  numBlocks_ = 0;
  for (auto &chunkInfo : previouslyTransferredChunks) {
    nextSeqId_ = std::max(nextSeqId_, chunkInfo.getSeqId() + 1);
    auto fileName = chunkInfo.getFileName();
    previouslyTransferredChunks_.insert(
        std::make_pair(std::move(fileName), std::move(chunkInfo)));
  }
  clearSourceQueue();
  // recreate the queue
  for (const auto metadata : sharedFileData_) {
    // TODO: do not notify inside createIntoQueueInternal. This method still
    // holds the lock, so no point in notifying
    createIntoQueueInternal(metadata);
  }
  enqueueFilesToBeDeleted();
}

std::unique_ptr<ByteSource> SourceQueue::getNextSource(
    ThreadCtx *callerThreadCtx, ErrorCode &status) {
  std::unique_ptr<ByteSource> source;
  while (true) {
    std::unique_lock<std::mutex> lock(mutex_);
    while (sourceQueue_.empty() && !initFinished_) {
      conditionNotEmpty_.wait(lock);
    }
    if (!failedSourceStats_.empty() || !failedDirectories_.empty()) {
      status = ERROR;
    } else {
      status = OK;
    }
    if (sourceQueue_.empty()) {
      return nullptr;
    }
    // using const_cast since priority_queue returns a const reference
    source = std::move(
        const_cast<std::unique_ptr<ByteSource> &>(sourceQueue_.top()));
    sourceQueue_.pop();
    if (sourceQueue_.empty() && initFinished_) {
      conditionNotEmpty_.notify_all();
    }
    lock.unlock();
    // try to open the source
    if (source->open(callerThreadCtx) == OK) {
      lock.lock();
      numBlocksDequeued_++;
      return source;
    }
    source->close();
    // we need to lock again as we will be adding element to failedSourceStats
    // vector
    lock.lock();
    failedSourceStats_.emplace_back(std::move(source->getTransferStats()));
  }
}


std::vector<TransferStats> &SourceQueue::getFailedSourceStats() {
  while (!sourceQueue_.empty()) {
    failedSourceStats_.emplace_back(
        std::move(sourceQueue_.top()->getTransferStats()));
    sourceQueue_.pop();
  }
  return failedSourceStats_;
}

std::vector<string> &SourceQueue::getFailedDirectories() {
  return failedDirectories_;
}


}
