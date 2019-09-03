
#pragma once

#include <wdt/WdtBase.h>
#include <wdt/util/FileCreator.h>
#include <chrono>
#include <iostream>
#include <memory>

namespace facebook {
namespace wdt {

class FileFileThread;
class TransferHistoryController;

class FileFile : public WdtBase {
 public:
  explicit FileFile(const WdtTransferRequest &transferRequest);

  FileFile(int port, int numSockets, const std::string &destDir);

  const WdtTransferRequest &init() override;

  ~FileFile() override;

  std::unique_ptr<TransferReport> finish() override;

  ErrorCode transferAsync() override;

  std::unique_ptr<TransferReport> transfer();

  Clock::time_point getEndTime();

  const std::string &getDestination() const;

  void setProgressReportIntervalMillis(const int progressReportIntervalMillis);

  bool hasNewTransferStarted() const;

  std::unique_ptr<TransferReport> getTransferReport();
 private:
  friend class FileFileThread;
  friend class QueueAbortChecker;

  ErrorCode validateTransferRequest() override;

  TransferStats getGlobalTransferStats() const;

  bool isSendFileChunks() const;

  bool isFileChunksReceived();

  std::atomic<bool> hasNewTransferStarted_{false};

  /// FileFile thread calls this method to set the file chunks info received
  /// from the receiver
  void setFileChunksInfo(std::vector<FileChunksInfo> &fileChunksInfoList);

  const std::vector<FileChunksInfo> &getFileChunksInfo() const;

  std::unique_ptr<FileCreator> &getFileCreator();

  /// Abort checker passed to DirectoryQueue. If all the network threads finish,
  /// directory discovery thread is also aborted
  class QueueAbortChecker : public IAbortChecker {
   public:
    explicit QueueAbortChecker(FileFile *worker) : worker_(worker) {
    }

    bool shouldAbort() const override {
      return (worker_->getTransferStatus() == FINISHED);
    }

   private:
    FileFile *worker_;
  };

  /// Abort checker shared with the directory queue
  QueueAbortChecker queueAbortChecker_;

  ErrorCode start();

  void endCurGlobalSession();

  void validateTransferStats(
      const std::vector<TransferStats> &transferredSourceStats,
      const std::vector<TransferStats> &failedSourceStats);

  void reportProgress();

  void progressTracker();

  std::thread progressTrackerThread_;

  void logPerfStats() const override;

  std::unique_ptr<DirectorySourceQueue> dirQueue_;

  int32_t numActiveThreads_{0};

  int progressReportIntervalMillis_;

  bool downloadResumptionEnabled_{false};

  bool fileChunksReceived_{false};

  bool isJoinable_{false};

  std::thread dirThread_;

  std::vector<std::unique_ptr<WdtThread>> workerThreads_;

  std::thread progressReporterThread_;

  void endCurTransfer();

  void startNewTransfer();

  /// Responsible for writing files on the disk
  std::unique_ptr<FileCreator> fileCreator_{nullptr};

  std::vector<FileChunksInfo> fileChunksInfo_;

  std::chrono::time_point<Clock> startTime_;
  std::chrono::time_point<Clock> endTime_;

  std::unique_ptr<TransferHistoryController> transferHistoryController_;

  void addCheckpoint(Checkpoint checkpoint);

  std::vector<Checkpoint> checkpoints_;

  std::unique_ptr<TransferLogManager> transferLogManager_;

  int backlog_;

  std::vector<std::unique_ptr<WdtThread>> receiverThreads_;

  std::string recoveryId_;

  void setRecoveryId(const std::string &recoveryId);

  int64_t getTransferConfig() const;

  void traverseDestinationDir(std::vector<FileChunksInfo> &fileChunksInfo);

};

}
}  // namespace facebook::wdt
