
#include <datamover/movers/FileFileThread.h>
#include <folly/Conv.h>
#include <folly/Memory.h>
#include <folly/ScopeGuard.h>
#include <folly/String.h>
#include <folly/hash/Checksum.h>
#include <folly/lang/Bits.h>
#include <datamover/util/FileWriter.h>
#include <sys/stat.h>

namespace facebook {
namespace wdt {

const FileFileThread::StateFunction FileFileThread::stateMap_[] = {
    &FileFileThread::copyFileChunk,
    &FileFileThread::checkForAbort,
    &FileFileThread::finishWithError};

void FileFileThread::start() {
  if (buf_ == nullptr) {
    WTLOG(ERROR) << "Unable to allocate buffer";
    threadStats_.setLocalErrorCode(MEMORY_ALLOCATION_ERROR);
    return;
  }

  controller_->executeAtStart([&]() { wdtParent_->startNewTransfer(); });

  FileFileState state = COPY_FILE_CHUNK;
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

ErrorCode FileFileThread::init() {
  return OK;
}

int32_t FileFileThread::getPort() const {
  return port_;
}

void FileFileThread::reset() {
  numRead_ = off_ = 0;
  //checkpointIndex_ = pendingCheckpointIndex_ = 0;
  //senderReadTimeout_ = senderWriteTimeout_ = -1;
  //curConnectionVerified_ = false;
  threadStats_.reset();
  //checkpoints_.clear();
  //newCheckpoints_.clear();
  //checkpoint_ = Checkpoint(getPort());
}

FileFileState FileFileThread::copyFileChunk() {
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

TransferStats FileFileThread::copyOneByteSource(
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


  WTVLOG(1) << "Read id:" << blockDetails.fileName
            << " size:" << blockDetails.dataSize << " ooff:" << oldOffset_;
  auto &fileCreator = wdtParent_->getFileCreator();
  FileWriter writer(*threadCtx_, &blockDetails, fileCreator.get());

  // writer.open() deletes files if status == TO_BE_DELETED
  // therefore if !(!delete_extra_files && status == TO_BE_DELETED)
  // we should skip writer.open() call altogether
  if (options_.delete_extra_files ||
      blockDetails.allocationStatus != TO_BE_DELETED) {
    if (writer.open() != OK) {
      threadStats_.setLocalErrorCode(FILE_WRITE_ERROR);
      return stats;
    }
  }

  int32_t checksum = 0;
  while (!source->finished()) {
    int64_t size;
    char *buffer = source->read(size);
    if (source->hasError()) {
      WTLOG(ERROR) << "Failed reading file " << source->getIdentifier()
                   << " for fd " << metadata.fd ;
      break;
    }
    WDT_CHECK(buffer && size > 0);

    auto throttler = wdtParent_->getThrottler();
    if (throttler) {
        // We might be reading more than we require for this file but
        // throttling should make sense for any additional bytes received
        // on the network
        throttler->limit(*threadCtx_, size);
    }

    ErrorCode code = ERROR;
    code = writer.write(buffer, size);
    if (code != OK) {
        threadStats_.setLocalErrorCode(code);
        return stats;
    }

    if (wdtParent_->getCurAbortCode() != OK) {
        WTLOG(ERROR) << "Thread marked for abort while processing "
                    << blockDetails.fileName << " " << blockDetails.seqId;
        threadStats_.setLocalErrorCode(ABORT);
        return stats;
    }

    stats.addDataBytes(size);
    actualSize += size;
  }

  const ErrorCode syncCode = writer.sync();
  if (syncCode != OK) {
      WTLOG(ERROR) << "could not sync " << blockDetails.fileName << " to disk";
      threadStats_.setLocalErrorCode(syncCode);
      return stats;
  }

  if (writer.getTotalWritten() != blockDetails.dataSize) {
      WTLOG(ERROR) << "TotalWriten: " << writer.getTotalWritten()
                   << " dataSize: " << blockDetails.dataSize;
      WTLOG(ERROR) << "could not read entire content for "
                  << blockDetails.fileName;
      threadStats_.setLocalErrorCode(BYTE_SOURCE_READ_ERROR);
      return stats;
  }

  if (getThreadAbortCode() != OK) {
    WTLOG(ERROR) << "Transfer aborted during block transfer "
                 << source->getIdentifier();
    stats.setLocalErrorCode(ABORT);
    stats.incrFailedAttempts();
    return stats;
  }
  const ErrorCode closeCode = writer.close();
  if (closeCode != OK) {
      WTLOG(ERROR) << "could not close " << blockDetails.fileName;
      threadStats_.setLocalErrorCode(closeCode);
      return stats;
  }

  WVLOG(2) << "completed " << blockDetails.fileName;
  stats.setLocalErrorCode(OK);
  stats.incrNumBlocks();
  stats.addEffectiveBytes(0, stats.getDataBytes());
  return stats;
};

FileFileState FileFileThread::checkForAbort() {
  WTLOG(INFO) << "entered CHECK_FOR_ABORT state";
  return COPY_FILE_CHUNK;
}

FileFileState FileFileThread::finishWithError() {
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

std::ostream &operator<<(std::ostream &os, const FileFileThread &workerThread) {
  os << "Thread[" << workerThread.threadIndex_
     << ", port: " << workerThread.port_ << "] ";
  return os;
}

ErrorCode FileFileThread::getThreadAbortCode() {
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

