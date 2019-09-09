
#pragma once

#include <wdt/WdtBase.h>
#include <wdt/util/TransferLogManager.h>
#include <chrono>
#include <iostream>
#include <memory>

namespace facebook {
namespace wdt {

class FileS3Thread;
class TransferHistoryController;

class FileS3 : public WdtBase {
 public:
  explicit FileS3(const WdtTransferRequest &transferRequest);

  FileS3(int port, int numSockets, const std::string &destDir);

  const WdtTransferRequest &init() override;

  ~FileS3() override;

  std::unique_ptr<TransferReport> finish() override;

  ErrorCode transferAsync() override;

  std::unique_ptr<TransferReport> transfer();

  Clock::time_point getEndTime();

  const std::string &getDestination() const;

  void setProgressReportIntervalMillis(const int progressReportIntervalMillis);

  bool hasNewTransferStarted() const;

  std::unique_ptr<TransferReport> getTransferReport();

  // Mutex lock to properly set upload multipart files
  std::mutex awsObjectMutex_;

  Aws::Auth::AWSCredentials awsClientCreds_;
  Aws::S3::S3Client s3_client_;
  Aws::Client::ClientConfiguration clientConfig_;

  class AwsObject {
   public:
    explicit AwsObject(int partNumber, int partTotal) :
        partNumber_(partNumber),
        partTotal_(partTotal),
        partsLeft_(partTotal),
        partStatus_(partTotal, false)
      {
    }


    void markPartUploaded(int partNumber){
        partsStatus.assign(partnumber, true);
        partsLeft--;
        uploadStarted = True;
    }

    void setMultipartKey(Aws::String multipartKey){
        multipartKey_ = multipartKey;
    }

    Aws::String getMultipartKey(){
        return multipartKey_;
    }

    void isFinished(){
        return !(bool)partsLeft;
    }

   private:

    /*
    int partsLeft(){
      int toUpload = 0;
      for (int isUploaded : partsStatus_) {
          if(!isuploaded){
              toUpload++;
          }
      }
      return toUpload;
    }
    */

    bool uploadStarted{false};
    bool uploadFinished{false};

    int partNumber_;
    int partTotal_
    std:list<bool> partsStatus_;

    Aws:String multipartKey{""};

  };

  // keep track of what parts of the file are uploaded and when
  // to do the multipart open and close
  std::unordered_map<std::string, AwsObject> awsObjectTracker_;

 private:
  friend class FileS3Thread;
  friend class QueueAbortChecker;

  ErrorCode validateTransferRequest() override;

  TransferStats getGlobalTransferStats() const;

  bool isSendFileChunks() const;

  bool isFileChunksReceived();

  std::atomic<bool> hasNewTransferStarted_{false};

  /// FileS3 thread calls this method to set the file chunks info received
  /// from the receiver
  void setFileChunksInfo(std::vector<FileChunksInfo> &fileChunksInfoList);

  const std::vector<FileChunksInfo> &getFileChunksInfo() const;

  /// Abort checker passed to DirectoryQueue. If all the network threads finish,
  /// directory discovery thread is also aborted
  class QueueAbortChecker : public IAbortChecker {
   public:
    explicit QueueAbortChecker(FileS3 *worker) : worker_(worker) {
    }

    bool shouldAbort() const override {
      return (worker_->getTransferStatus() == FINISHED);
    }

   private:
    FileS3 *worker_;
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

  std::vector<FileChunksInfo> fileChunksInfo_;

  std::chrono::time_point<Clock> startTime_;
  std::chrono::time_point<Clock> endTime_;

  std::unique_ptr<TransferHistoryController> transferHistoryController_;

  void addCheckpoint(Checkpoint checkpoint);

  std::vector<Checkpoint> checkpoints_;

  std::unique_ptr<TransferLogManager> transferLogManager_;

  int backlog_;


  std::string recoveryId_;

  void setRecoveryId(const std::string &recoveryId);

  int64_t getTransferConfig() const;

  void traverseDestinationDir(std::vector<FileChunksInfo> &fileChunksInfo);


};

}
}  // namespace facebook::wdt
