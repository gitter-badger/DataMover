
#pragma once
#include <folly/Conv.h>
#include <wdt/workers/FileWdt.h>
#include <wdt/WdtThread.h>
#include <wdt/util/IClientSocket.h>
#include <wdt/util/ThreadTransferHistory.h>
#include <thread>

namespace facebook {
namespace wdt {

class DirectorySourceQueue;

/// state machine states
enum FileFileState {
  INIT,
  COPY_FILE_CHUNK,
  CHECK_FOR_ABORT,
  FINISH_WITH_ERROR,
  END
};

class FileFileThread : public WdtThread {
 public:

  class SocketAbortChecker : public IAbortChecker {
   public:
    explicit SocketAbortChecker(FileFileThread *threadPtr)
        : threadPtr_(threadPtr) {
    }

    bool shouldAbort() const override {
      return (threadPtr_->getThreadAbortCode() != OK);
    }

   private:
    FileFileThread *threadPtr_{nullptr};
  };

  FileFileThread(
            FileFile *worker,
            int threadIndex,
            ThreadsController *threadsController)
      : WdtThread(
            worker->options_, threadIndex, port,
            worker->getProtocolVersion(),
            threadsController),
        wdtParent_(worker),
        dirQueue_(worker->dirQueue_.get()),
        transferHistoryController_(worker->transferHistoryController_.get()) {

    controller_->registerThread(threadIndex_);
    transferHistoryController_->addThreadHistory(port_, threadStats_);
    threadAbortChecker_ = std::make_unique<SocketAbortChecker>(this);
    threadCtx_->setAbortChecker(threadAbortChecker_.get());
    threadStats_.setId(folly::to<std::string>(threadIndex_));
    isTty_ = isatty(STDERR_FILENO);
  }

  ~FileWdtThread() override {
  }

  typedef FileFileState (FileFileThread::*StateFunction)();

  ErrorCode init() override;

  void reset() override;

  ErrorCode getThreadAbortCode();

 private:
  /// Overloaded operator for printing thread info
  friend std::ostream &operator<<(std::ostream &os,
                                  const FileWdtThread &senderThread);

  FileFile *wdtParent_;

  void setFooterType();

  void start() override;

  static const StateFunction stateMap_[];

  ThreadTransferHistory &getTransferHistory() {
    return transferHistoryController_->getTransferHistory(port_);
  }

  FileWdtState checkForAbort();

  TransferStats sendOneByteSource(const std::unique_ptr<ByteSource> &source,
                                  ErrorCode transferStatus);

  DirectorySourceQueue *dirQueue_;

  TransferHistoryController *transferHistoryController_;


