/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#pragma once

#include <datamover/Protocol.h>
#include <datamover/WdtTransferRequest.h>
#include <datamover/endpoints/SourceQueue.h>
#include <datamover/endpoints/file/FileByteSource.h>
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
class S3SourceQueue : public SourceQueue {
 public:
  /**
   * Create a S3SourceQueue.
   * Call buildQueueSynchronously() or buildQueueAsynchronously() separately
   * to actually recurse over the root directory gather files and sizes.
   *
   * @param options               options to use
   * @param rootDir               root directory to recurse on
   * @param abortChecker          abort checker
   */
  S3SourceQueue(const WdtOptions &options, const std::string &rootDir,
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

  ~S3SourceQueue() override;

  /**
   * checks to see if path in the s3 bucket exists
   * path.
   * @return    true if successful, false on error (logged)
   */
  bool setRootDir(const std::string &newRootDir);

 private:

  //S3 client connection
  Aws::S3::S3Client s3_client_;

  //default s3 options
  Aws::SDKOptions aws_options;

  //S3 credenchals
  Aws::Auth::AWSCredentials awsClientCreds_;

  Aws::Client::ClientConfiguration awsClientConfig_;

  /**
   * No Op, not needed for S3
   *
   * @return                realpath or empty string on error (logged)
   */
  std::string resolvePath(const std::string &path);

  /**
   * Traverse rootDir_ to gather files to enqueue
   *
   * @return                true on success, false on error
   */
  bool explore();

  /**
   * Stat the input files get size and populate queue
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

  /// if file deletion is enabled, extra files to be deleted are enqueued. This
  /// method should be called while holding the lock
  void enqueueFilesToBeDeleted();

  /// A map from relative file name to previously received chunks
  std::unordered_map<std::string, FileChunksInfo> previouslyTransferredChunks_;

};
}  // namespace datamover
