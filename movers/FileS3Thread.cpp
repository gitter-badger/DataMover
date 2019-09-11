
#include <wdt/movers/FileS3Thread.h>
#include <folly/Conv.h>
#include <folly/Memory.h>
#include <folly/ScopeGuard.h>
#include <folly/String.h>
#include <folly/lang/Bits.h>
#include <wdt/util/FileWriter.h>
#include <openssl/md5.h>
#include <sys/stat.h>


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

  controller_->executeAtStart([&]() { moverParent_->startNewTransfer(); });

  FileS3State state = COPY_FILE_CHUNK;
  while (true) {
    ErrorCode abortCode = moverParent_->getCurAbortCode();
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
  //controller_->executeAtEnd([&]() { moverParent_->endCurGlobalSession(); });
  controller_->executeAtEnd([&]() { moverParent_->endCurTransfer(); });
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
  /*
  BlockDetails blockDetails;
  blockDetails.fileName = metadata.relPath;
  blockDetails.seqId = metadata.seqId;
  blockDetails.fileSize = metadata.size;
  blockDetails.offset = source->getOffset();
  blockDetails.number = source->getBlockNumber();
  blockDetails.total = source->getBlockTotal();
  blockDetails.dataSize = expectedSize;
  blockDetails.allocationStatus = metadata.allocationStatus;
  blockDetails.prevSeqId = metadata.prevSeqId;
  */


  WLOG(INFO) << " Read id:" << metadata.relPath
            << " size:" << expectedSize << " ooff:" << oldOffset_;

  // FIXME: remove this loop?
  while (!source->finished()) {
    int64_t size;
    char *buffer = source->read(size);
    WLOG(INFO) << "Read size: " << size;
    if (source->hasError()) {
      WTLOG(ERROR) << "Failed reading file " << source->getIdentifier()
                   << " for fd " << metadata.fd ;
      break;
    }

    auto throttler = moverParent_->getThrottler();
    if (throttler) {
        // We might be reading more than we require for this file but
        // throttling should make sense for any additional bytes received
        // on the network
        throttler->limit(*threadCtx_, size);
    }

    //////////////////////////////// AWS

    S3Writer writer(*threadCtx_, *source, &*moverParent_);
    if(!writer.open()){
        threadStats_.setLocalErrorCode(ABORT);
        stats.incrFailedAttempts();
        return stats;
    }

    if(!writer.write(buffer, size)){
        threadStats_.setLocalErrorCode(ABORT);
        stats.incrFailedAttempts();
        return stats;
    }

    if(!writer.close()){
        threadStats_.setLocalErrorCode(ABORT);
        stats.incrFailedAttempts();
        return stats;
    }
    stats.addDataBytes(size);

    // TODO: actually validate the etag
    //
    //////////////////////////////// AWS

    if (moverParent_->getCurAbortCode() != OK) {
        WTLOG(ERROR) << "Thread marked for abort while processing "
                    << metadata.relPath << " " << metadata.seqId;
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

  WVLOG(2) << "completed " << metadata.relPath;
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
  moverParent_->addCheckpoint(checkpoint_);
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
  ErrorCode globalAbortCode = moverParent_->getCurAbortCode();
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

