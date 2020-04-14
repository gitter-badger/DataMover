
#include <datamover/endpoints/SourceQueue.h>

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

void SourceQueue::setIncludePattern(const string &includePattern) {
  includePattern_ = includePattern;
}

void SourceQueue::setExcludePattern(const string &excludePattern) {
  excludePattern_ = excludePattern;
}

void SourceQueue::setPruneDirPattern(const string &pruneDirPattern) {
  pruneDirPattern_ = pruneDirPattern;
}

void SourceQueue::setBlockSizeMbytes(int64_t blockSizeMbytes) {
  blockSizeMbytes_ = blockSizeMbytes;
}



void SourceQueue::setFileInfo(
    const std::vector<WdtFileInfo> &fileInfo) {
  fileInfo_ = fileInfo;
  exploreDirectory_ = false;
}

const std::vector<WdtFileInfo> &SourceQueue::getFileInfo() const {
  return fileInfo_;
}

std::vector<SourceMetaData *>
    &SourceQueue::getDiscoveredFilesMetaData() {
  return sharedFileData_;
}

std::pair<int64_t, ErrorCode> SourceQueue::getNumBlocksAndStatus()
    const {
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

}
