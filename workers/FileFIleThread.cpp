
#include <wdt/workers/WdtFileThread.h>
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
    &FileFileThread::init,
    &FileFileThread::copyFileChunk,
    &FileFileThread::checkForAbort,
    &FileFileThread::finishWidthError};

void WdtFileThread::start() {
  if (buf_ == nullptr) {
    WTLOG(ERROR) << "Unable to allocate buffer";
    threadStats_.setLocalErrorCode(MEMORY_ALLOCATION_ERROR);
    return;
  }
  WdtFileState state = INIT;
  while (true) {
    ErrorCode abortCode = wdtParent_->getCurAbortCode();
    if (abortCode != OK) {
      WTLOG(ERROR) << "Transfer aborted " << socket_->getPort() << " "
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

FileWdtState FileWdtThread::sendBlocks() {
  WTVLOG(1) << "entered SEND_BLOCKS state";
  ThreadTransferHistory &transferHistory = getTransferHistory();
  if (threadProtocolVersion_ >= Protocol::RECEIVER_PROGRESS_REPORT_VERSION &&
      !totalSizeSent_ && dirQueue_->fileDiscoveryFinished()) {
    return SEND_SIZE_CMD;
  }
  ErrorCode transferStatus;
  std::unique_ptr<ByteSource> source =
      dirQueue_->getNextSource(threadCtx_.get(), transferStatus);
  if (!source) {
    // try to read any buffered heart-beats
    readHeartBeats();

    return SEND_DONE_CMD;
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
    return CHECK_FOR_ABORT;
  }
  return SEND_BLOCKS;
}

TransferStats FileWdtThread::copyOneByteSource(
    const std::unique_ptr<ByteSource> &source, ErrorCode transferStatus) {
  TransferStats stats;
  char headerBuf[Protocol::kMaxHeader];
  int64_t off = 0;
  headerBuf[off++] = Protocol::FILE_CMD;
  headerBuf[off++] = transferStatus;
  char *headerLenPtr = headerBuf + off;
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
  const auto encryptionType = socket_->getEncryptionType();
  /* FIXME Dose i need?
  auto writtenGuard = folly::makeGuard([&] {
    if (!encryptionTypeToTagLen(encryptionType) && footerType_ == NO_FOOTER) {
      // if encryption doesn't have tag verification and checksum verification
      // is disabled, we can consider bytes received before connection break as
      // valid
      checkpoint_.setLastBlockDetails(blockDetails.seqId, blockDetails.offset,
                                      writer.getTotalWritten());
      threadStats_.addEffectiveBytes(headerBytes, writer.getTotalWritten());
    }
  });
  */

  // writer.open() deletes files if status == TO_BE_DELETED
  // therefore if !(!delete_extra_files && status == TO_BE_DELETED)
  // we should skip writer.open() call altogether
  if (options_.delete_extra_files ||
      blockDetails.allocationStatus != TO_BE_DELETED) {
    if (writer.open() != OK) {
      threadStats_.setLocalErrorCode(FILE_WRITE_ERROR);
      return SEND_ABORT_CMD;
    }
  }

  if (footerType_ == CHECKSUM_FOOTER) {
    checksum = folly::crc32c((const uint8_t *)(buf_ + off_), toWrite, checksum);
  }
  auto throttler = wdtParent_->getThrottler();
  if (throttler) {
    // We might be reading more than we require for this file but
    // throttling should make sense for any additional bytes received
    // on the network
    throttler->limit(*threadCtx_, toWrite + headerBytes);
  }

  sendHeartBeat();

  ErrorCode code = ERROR;
  code = writer.write(buf_ + off_, toWrite);
  if (code != OK) {
    threadStats_.setLocalErrorCode(code);
    return SEND_ABORT_CMD;
  }
  threadStats_.addDataBytes(writer.getTotalWritten());

  if (wdtParent_->getCurAbortCode() != OK) {
    WTLOG(ERROR) << "Thread marked for abort while processing "
                 << blockDetails.fileName << " " << blockDetails.seqId
                 << " port : " << socket_->getPort();
    threadStats_.setLocalErrorCode(ABORT);
    return FINISH_WITH_ERROR;
  }

  if (throttler) {
    // We only know how much we have read after we are done calling
    // readAtMost. Call throttler with the bytes read off_ the wire.
    throttler->limit(*threadCtx_, nres);
  }

  // Sync the writer to disk and close it. We need to check for error code each
  // time, otherwise we would move forward with corrupted files.
  const ErrorCode syncCode = writer.sync();
  if (syncCode != OK) {
    WTLOG(ERROR) << "could not sync " << blockDetails.fileName << " to disk";
    threadStats_.setLocalErrorCode(syncCode);
    return SEND_ABORT_CMD;
  }
  const ErrorCode closeCode = writer.close();
  if (closeCode != OK) {
    WTLOG(ERROR) << "could not close " << blockDetails.fileName;
    threadStats_.setLocalErrorCode(closeCode);
    return SEND_ABORT_CMD;
  }

  if (writer.getTotalWritten() != blockDetails.dataSize) {
    WTLOG(ERROR) << "could not read entire content for "
                 << blockDetails.fileName << " port " << socket_->getPort();
    threadStats_.setLocalErrorCode(SOCKET_READ_ERROR);
    return ACCEPT_WITH_TIMEOUT;
  }
  //writtenGuard.dismiss();
  WVLOG(2) << "completed " << blockDetails.fileName << " off: " << off_
           << " numRead: " << numRead_;


};


}
}

