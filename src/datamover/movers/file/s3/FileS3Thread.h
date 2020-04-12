
#pragma once
#include <folly/Conv.h>
#include <datamover/WdtBase.h>
#include <datamover/WdtThread.h>
#include <datamover/movers/FileS3.h>
#include <datamover/util/ThreadTransferHistory.h>
#include <datamover/endpoints/s3/S3Writer.h>
#include <thread>

namespace datamover {

class DirectorySourceQueue;

/// state machine states
enum FileS3State {
  COPY_FILE_CHUNK,
  CHECK_FOR_ABORT,
  FINISH_WITH_ERROR,
  END
};

class FileS3Thread : public WdtThread {
 public:

  /// Identifiers for the barriers used in the thread
  enum SENDER_BARRIERS { VERSION_MISMATCH_BARRIER, NUM_BARRIERS };

  /// Identifiers for the funnels used in the thread
  enum SENDER_FUNNELS { VERSION_MISMATCH_FUNNEL, NUM_FUNNELS };

  /// Identifier for the condition wrappers used in the thread
  enum SENDER_CONDITIONS { NUM_CONDITIONS };

  class FileAbortChecker : public IAbortChecker {
   public:
    explicit FileAbortChecker(FileS3Thread *threadPtr)
        : threadPtr_(threadPtr) {
    }

    bool shouldAbort() const override {
      return (threadPtr_->getThreadAbortCode() != OK);
    }

   private:
    FileS3Thread *threadPtr_{nullptr};
  };

  FileS3Thread( FileS3 *worker, int threadIndex, int32_t port,
            ThreadsController *threadsController)
      : WdtThread(
            worker->options_, threadIndex, port,
            worker->getProtocolVersion(),
            threadsController),
        moverParent_(worker),
        dirQueue_(worker->dirQueue_.get()),
        transferHistoryController_(worker->transferHistoryController_.get()) {

    controller_->registerThread(threadIndex_);
    transferHistoryController_->addThreadHistory(port_, threadStats_);
    threadAbortChecker_ = std::make_unique<FileAbortChecker>(this);
    threadCtx_->setAbortChecker(threadAbortChecker_.get());
    threadStats_.setId(folly::to<std::string>(threadIndex_));
    isTty_ = isatty(STDERR_FILENO);
  }

  ~FileS3Thread() override {
  }

  FileS3State copyFileChunk();

  FileS3State finishWithError();

  TransferStats copyOneByteSource();
  TransferStats copyOneByteSource(const std::unique_ptr<ByteSource> &source,
                                  ErrorCode transferStatus);

  int64_t numRead_{0};
  int64_t off_{0};
  int64_t oldOffset_{0};

  typedef FileS3State (FileS3Thread::*StateFunction)();

  ErrorCode init() override;

  void reset() override;

  int32_t getPort() const override;

  ErrorCode getThreadAbortCode();

 private:
  /// Overloaded operator for printing thread info
  friend std::ostream &operator<<(std::ostream &os,
                                  const FileS3Thread &workerThread);

  FileS3 *moverParent_;

  void setFooterType();

  void start() override;

  static const StateFunction stateMap_[];

  bool isTty_{false};

  int checkpointIndex_{0};

  Checkpoint checkpoint_;

  std::vector<Checkpoint> newCheckpoints_;

  ThreadTransferHistory &getTransferHistory() {
    return transferHistoryController_->getTransferHistory(port_);
  }

  FileS3State checkForAbort();

  DirectorySourceQueue *dirQueue_;

  TransferHistoryController *transferHistoryController_;

  std::unique_ptr<IAbortChecker> threadAbortChecker_{nullptr};

};
}
