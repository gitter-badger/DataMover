/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#pragma once

#include <memory>
#include <mutex>

#include <datamover/ByteSource.h>

namespace datamover {

/**
 * Interface for consuming data from multiple ByteSource's.
 * Call getNextSource() repeatedly to get new sources to consume data
 * from, until finished() returns true.
 *
 * This class is thread-safe, i.e. multiple threads can consume sources
 * in parallel and terminate once finished() returns true. Each source
 * is guaranteed to be consumed exactly once.
 */
class SourceQueue {
 public:
  virtual ~SourceQueue() {
  }

  /**
   * initial creation from either explore or enqueue files, uses
   * createIntoQueueInternal to create blocks
   *
   * @param fullPath             full path of the file to be added
   * @param fileInfo             Information about file
   */
  virtual void createIntoQueue(const std::string &fullPath, WdtFileInfo &fileInfo);

  /**
   * initial creation from either explore or enqueue files - always increment
   * numentries. Lock must be held before calling this.
   *
   * @param metadata             file meta-data
   */
  virtual void createIntoQueueInternal(SourceMetaData *metadata);

  /**
   * Allows to change the root directory, must not be empty, trailing
   * slash is automatically added if missing. Can be relative.
   * if follow symlink is set the directory will be resolved as absolute
   * path.
   * @return    true if successful, false on error (logged)
   */
  virtual bool setRootDir(const std::string &newRootDir);

  /// @return   discovered files metadata
  std::vector<SourceMetaData *> &getDiscoveredFilesMetaData();

  /// @return true if all the files have been discovered, false otherwise
  bool fileDiscoveryFinished();

  /// @return         total number of blocks and status of the transfer
  std::pair<int64_t, ErrorCode> getNumBlocksAndStatus();

  /// @return         perf report
  const PerfStatReport &getPerfReport();

  /**
   * Sets regex representing files to include for transfer
   *
   * @param includePattern          file inclusion regex
   */
  void setIncludePattern(const std::string &includePattern);

  /**
   * Sets regex representing files to exclude for transfer
   *
   * @param excludePattern          file exclusion regex
   */
 void setExcludePattern(const std::string &excludePattern);

    /**
   * priority queue of sources. Sources are first ordered by increasing
   * failedAttempts, then by decreasing size. If sizes are equal(always for
   * blocks), sources are ordered by offset. This way, we ensure that all the
   * threads in the receiver side are not writing to the same file at the same
   * time.
   */
  std::priority_queue<std::unique_ptr<ByteSource>,
                      std::vector<std::unique_ptr<ByteSource>>,
                      SourceComparator>
      sourceQueue_;
  
  /// protects initCalled_/initFinished_/sourceQueue_/failedSourceStats_
  mutable std::mutex mutex_;

  /// condition variable indicating sourceQueue_ is not empty
  mutable std::condition_variable conditionNotEmpty_;

  /// Indicates whether init() has been called to prevent multiple calls
  bool initCalled_{false};

  /// Indicates whether call to init() has finished
  bool initFinished_{false};
  
 
  std::unique_ptr<ThreadCtx> threadCtx_{nullptr};

  /// root directory to recurse on if fileInfo_ is empty
  std::string rootDir_;

  /// regex representing files to include
  std::string udePattern_;

  /// regex representing files to exclude
  std::string excludePattern_;

  /// Block size in mb
  int64_t blockSizeMbytes_{0};
  /// regex representing directories to prune
  


  /// List of files to enqueue instead of recursing over rootDir_.
  std::vector<WdtFileInfo> fileInfo_;
  
  /// @return true iff no more sources to read from
  bool finished();

  /**
   * @param callerThreadCtx context of the calling thread
   * @param status          this variable is set to the status of the transfer
   
   * @return next FileByteSource to consume or nullptr when finished
   */
  std::unique_ptr<ByteSource> getNextSource(ThreadCtx *callerThreadCtx,
                                            ErrorCode &status);

  /// @return         total number of files processed/enqueued
  int64_t getCount();

  /// @return         total size of files processed/enqueued
  int64_t getTotalSize();

  /**
   * returns sources to the queue, checks for fail/retries, doesn't increment
   * numentries
   *
   * @param sources               sources to be returned to the queue
   */
  void returnToQueue(std::vector<std::unique_ptr<ByteSource>> &sources);

  /**
   * returns a source to the queue, checks for fail/retries, doesn't increment
   * numentries
   *
   * @param source                source to be returned to the queue
   */
  void returnToQueue(std::unique_ptr<ByteSource> &source);

  /**
   * when adding multiple files, we have the option of using notify_one multiple
   * times or notify_all once. Depending on number of added sources, this
   * function uses either notify_one or notify_all
   *
   * @param addedSource     number of sources added
   */
  void smartNotify(int32_t addedSource);

};
}
