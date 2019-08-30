
#include <wdt/workers/FileFileThread.h>
#include <folly/Conv.h>
#include <folly/Memory.h>
#include <folly/ScopeGuard.h>
#include <folly/String.h>
#include <folly/hash/Checksum.h>
#include <folly/lang/Bits.h>
#include <wdt/util/FileWriter.h>

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
  controller_->executeAtEnd([&]() { wdtParent_->endCurGlobalSession(); });
  WTLOG(INFO) << threadStats_;
}

ErrorCode FileFileThread::init() {
  return OK;
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
            << " size:" << blockDetails.dataSize << " ooff:" << oldOffset_
            << " off_: " << off_ << " numRead_: " << numRead_;
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
  int64_t remainingData = numRead_ + oldOffset_ - off_;
  int64_t toWrite = remainingData;
  WDT_CHECK(remainingData >= 0);
  if (remainingData >= blockDetails.dataSize) {
    toWrite = blockDetails.dataSize;
  }
  threadStats_.addDataBytes(toWrite);
  //checksum = folly::crc32c((const uint8_t *)(buf_ + off_), toWrite, checksum);
  auto throttler = wdtParent_->getThrottler();
  if (throttler) {
    // We might be reading more than we require for this file but
    // throttling should make sense for any additional bytes received
    // on the network
    throttler->limit(*threadCtx_, toWrite);
  }

  ErrorCode code = ERROR;
  code = writer.write(buf_ + off_, toWrite);
  if (code != OK) {
    threadStats_.setLocalErrorCode(code);
    return stats;
  }
  threadStats_.addDataBytes(writer.getTotalWritten());

  if (wdtParent_->getCurAbortCode() != OK) {
    WTLOG(ERROR) << "Thread marked for abort while processing "
                 << blockDetails.fileName << " " << blockDetails.seqId;
    threadStats_.setLocalErrorCode(ABORT);
    return stats;
  }

  // Sync the writer to disk and close it. We need to check for error code each
  // time, otherwise we would move forward with corrupted files.
  const ErrorCode syncCode = writer.sync();
  if (syncCode != OK) {
    WTLOG(ERROR) << "could not sync " << blockDetails.fileName << " to disk";
    threadStats_.setLocalErrorCode(syncCode);
    return stats;
  }
  const ErrorCode closeCode = writer.close();
  if (closeCode != OK) {
    WTLOG(ERROR) << "could not close " << blockDetails.fileName;
    threadStats_.setLocalErrorCode(closeCode);
    return stats;
  }

  if (writer.getTotalWritten() != blockDetails.dataSize) {
    WTLOG(ERROR) << "could not read entire content for "
                 << blockDetails.fileName;
    threadStats_.setLocalErrorCode(BYTE_SOURCE_READ_ERROR);
    return stats;
  }
  //writtenGuard.dismiss();
  WVLOG(2) << "completed " << blockDetails.fileName << " off: " << off_
           << " numRead: " << numRead_;
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

}
}

