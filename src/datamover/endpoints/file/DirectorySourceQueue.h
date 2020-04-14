/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#pragma once

#include <dirent.h>
#include <glog/logging.h>
#include <algorithm>
#include <condition_variable>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>

#include <datamover/Protocol.h>
#include <datamover/endpoints/SourceQueue.h>
#include <datamover/WdtTransferRequest.h>
#include <datamover/endpoints/file/FileByteSource.h>

namespace datamover {
/**
 * SourceQueue that returns all the regular files under a given directory
 * (recursively) as individual FileByteSource objects, sorted by decreasing
 * file size.
 *
 * TODO: The actual building of the queue is specific to this implementation
 * which may or may not make it easy to plug a different implementation
 * (as shown by the current implementation of Sender.cpp)
 */
class DirectorySourceQueue : public SourceQueue {
 public:
  /**
   * Create a DirectorySourceQueue.
   * Call buildQueueSynchronously() or buildQueueAsynchronously() separately
   * to actually recurse over the root directory gather files and sizes.
   *
   * @param options               options to use
   * @param rootDir               root directory to recurse on
   * @param abortChecker          abort checker
   */
  DirectorySourceQueue(const WdtOptions &options, const std::string &rootDir,
                       IAbortChecker const *abortChecker);

  /**
   * Recurse over given root directory, gather data about regular files and
   * initialize internal data structures. getNextSource() will return sources
   * as this call discovers them.
   *
   * This should only be called once. Subsequent calls will do nothing and
   * return false. In case it is called from multiple threads, one of them
   * will do initialization while the other calls will fail.
   *
   * This is synchronous in the succeeding thread - it will block until
   * the directory is completely discovered. Use buildQueueAsynchronously()
   * for async fetch from parallel thread.
   *
   * @return          true iff initialization was successful and hasn't
   *                  been done before
   */
  bool buildQueueSynchronously();

  /**
   * Starts a new thread to build the queue @see buildQueueSynchronously()
   * @return the created thread (to be joined if needed)
   */
  std::thread buildQueueAsynchronously();


  /**
   * Sets regex representing directories to exclude for transfer
   *
   * @param pruneDirPattern         directory exclusion regex
   */
  void setPruneDirPattern(const std::string &pruneDirPattern);

  /**
   * Sets the number of consumer threads for this queue. used as threshold
   * between notify and notifyAll
   */
  void setNumClientThreads(int64_t numClientThreads) {
    numClientThreads_ = numClientThreads;
  }

  /**
   * Sets the count and trigger for files to open during discovery
   * (negative is keep opening until we run out of fd, positive is how
   * many files we can still open, 0 is stop opening files)
   */
  void setOpenFilesDuringDiscovery(int64_t openFilesDuringDiscovery) {
    openFilesDuringDiscovery_ = openFilesDuringDiscovery;
  }
  /**
   * If setOpenFilesDuringDiscovery is not zero, open files using direct
   * mode.
   */
  void setDirectReads(bool directReads) {
    directReads_ = directReads;
  }

  /// enable extra file deletion in the receiver side
  void enableFileDeletion() {
    deleteFiles_ = true;
  }

  /**
   * Stat the FileInfo input files (if their size aren't already specified) and
   * insert them in the queue
   *
   * @param fileInfo              files to transferred
   */
  void setFileInfo(const std::vector<WdtFileInfo> &fileInfo);

  /// @param blockSizeMbytes    block size in Mbytes
  void setBlockSizeMbytes(int64_t blockSizeMbytes);

  /// Get the file info in this directory queue
  const std::vector<WdtFileInfo> &getFileInfo() const;

  /**
   * Sets whether to follow symlink or not
   *
   * @param followSymlinks        whether to follow symlink or not
   */
  void setFollowSymlinks(bool followSymlinks);

  /**
   * sets chunks which were sent in some previous transfer
   *
   * @param previouslyTransferredChunks   previously sent chunk info
   */
  void setPreviouslyReceivedChunks(
      std::vector<FileChunksInfo> &previouslyTransferredChunks);

  /**
   * Returns list of files which were not transferred. It empties the queue and
   * adds queue entries to the failed file list. This function should be called
   * after all the sending threads have finished execution
   *
   * @return                      stats for failed sources
   */
  std::vector<TransferStats> &getFailedSourceStats();

  /// @return   returns list of directories which could not be opened
  std::vector<std::string> &getFailedDirectories();

  /// @return   number of bytes previously sent
  int64_t getPreviouslySentBytes() const;

  ~DirectorySourceQueue() override;

  /// Returns the time it took to traverse the directory tree
  double getDirectoryTime() const {
    return directoryTime_;
  }

  /**
   * Allows to change the root directory, must not be empty, trailing
   * slash is automatically added if missing. Can be relative.
   * if follow symlink is set the directory will be resolved as absolute
   * path.
   * @return    true if successful, false on error (logged)
   */
  bool setRootDir(const std::string &newRootDir);

 private:
  /**
   * Resolves a symlink.
   *
   * @return                realpath or empty string on error (logged)
   */
  std::string resolvePath(const std::string &path);

  /**
   * Traverse rootDir_ to gather files and sizes to enqueue
   *
   * @return                true on success, false on error
   */
  bool explore();

  /**
   * Stat the input files and populate queue
   * @return                true on success, false on error
   */
  bool enqueueFiles();

  /**
   * initial creation from either explore or enqueue files, uses
   * createIntoQueueInternal to create blocks
   *
   * @param fullPath             full path of the file to be added
   * @param fileInfo             Information about file
   */
  void createIntoQueue(const std::string &fullPath, WdtFileInfo &fileInfo);

  /**
   * initial creation from either explore or enqueue files - always increment
   * numentries. Lock must be held before calling this.
   *
   * @param metadata             file meta-data
   */
  void createIntoQueueInternal(SourceMetaData *metadata);


  /// Removes all elements from the source queue
  void clearSourceQueue();

  /// if file deletion is enabled, extra files to be deleted are enqueued. This
  /// method should be called while holding the lock
  void enqueueFilesToBeDeleted();
 
  struct SourceComparator {
    bool operator()(const std::unique_ptr<ByteSource> &source1,
                    const std::unique_ptr<ByteSource> &source2) {
      bool toBeDeleted1 =
          (source1->getMetaData().allocationStatus == TO_BE_DELETED);
      bool toBeDeleted2 =
          (source2->getMetaData().allocationStatus == TO_BE_DELETED);
      if (toBeDeleted1 != toBeDeleted2) {
        // always send files to be deleted first
        return toBeDeleted2;
      }

      auto retryCount1 = source1->getTransferStats().getFailedAttempts();
      auto retryCount2 = source2->getTransferStats().getFailedAttempts();
      if (retryCount1 != retryCount2) {
        return retryCount1 > retryCount2;
      }
      if (source1->getSize() != source2->getSize()) {
        return source1->getSize() < source2->getSize();
      }
      if (source1->getOffset() != source2->getOffset()) {
        return source1->getOffset() > source2->getOffset();
      }
      return source1->getIdentifier() > source2->getIdentifier();
    }
  };

  /// Transfer stats for sources which are not transferred
  std::vector<TransferStats> failedSourceStats_;

  /// directories which could not be opened
  std::vector<std::string> failedDirectories_;

  /// Total number of files that have passed through the queue
  int64_t numEntries_{0};

  /// Seq-id of the next file to be inserted into the queue
  /// first valid seq is 1 so we can use 0 as unintilized/invalid in protocol.h
  int64_t nextSeqId_{1};

  /// total number of blocks that have passed through the queue. Even when
  /// blocks are actually disabled, our code internally treats files like single
  /// blocks. So, numBlocks_ >= numFiles_.
  int64_t numBlocks_{0};

  /// Total size of entries/files that have passed through the queue
  int64_t totalFileSize_{0};

  /// Number of blocks dequeued
  int64_t numBlocksDequeued_{0};

  /// Whether to follow symlinks or not
  bool followSymlinks_{false};

  /// shared file data. This are used during transfer to add blocks
  /// contribution
  std::vector<SourceMetaData *> sharedFileData_;

  /// A map from relative file name to previously received chunks
  std::unordered_map<std::string, FileChunksInfo> previouslyTransferredChunks_;

  /// Stores the time difference between the start and the end of the
  /// traversal of directory
  double directoryTime_{0};

  /// Number of bytes previously sent
  int64_t previouslySentBytes_{0};

  /**
   * Count and trigger of files to open (negative is keep opening until we run
   * out of fd, positive is how many files we can still open, 0 is stop opening
   * files).
   * Sender only (Receiver download resumption directory discovery should not
   * open files).
   */
  int32_t openFilesDuringDiscovery_{0};
  /// Should the WdtFileInfo created during discovery have direct read mode set
  bool directReads_{false};

  // Number of files opened
  int64_t numFilesOpened_{0};
  // Number of files opened with odirect
  int64_t numFilesOpenedWithDirect_{0};
  // Number of consumer threads (to tell between notify/notifyall)
  int64_t numClientThreads_{1};
  // Should we explore or use fileInfo
  bool exploreDirectory_{true};
  /// delete extra files in the receiver side
  bool deleteFiles_{false};
};
}
