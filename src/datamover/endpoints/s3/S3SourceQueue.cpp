/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#include <datamover/Protocol.h>
#include <datamover/WdtTransferRequest.h>
#include <datamover/endpoints/file/S3SourceQueue.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>
#include <memory>
#include <regex>
#include <set>
#include <utility>

// NOTE: this should remain standalone code and not use WdtOptions directly
// also note this is used not just by the Sender but also by the receiver
// (so code like opening files during discovery is disabled by default and
// no reading the config directly from the options and only set by the Sender)

namespace datamover {
using std::string;

WdtFileInfo::WdtFileInfo(const std::string &name, std::string &etag)
    : fileName(name), s3Etag(etag) {
}

S3SourceQueue::S3SourceQueue(const WdtOptions &options,
                             const string &rootDir,
                             IAbortChecker const *abortChecker) {
  threadCtx_ =
      std::make_unique<ThreadCtx>(options, /* do not allocate buffer */ false);
  threadCtx_->setAbortChecker(abortChecker);
  setRootDir(rootDir);

  // create aws creds object
  awsClientCreds_ = Aws::Auth::AWSCredentials(options.awsAccessKeyId,
                                              awsSecretAccessKey);

  //The AWS SDK for C++ must be initialized by calling Aws::InitAPI.
  Aws::InitAPI(aws_options_);

  awsClientConfig_.region = options.awsRegion;
  awsClientConfig_.scheme = options.awsScheme;
  awsClientConfig_.endpointOverride = options.awsEndpointOverride;
  awsClientConfig_.verifySSL = options.awsVerifySSL;
  awsClientConfig_.requestTimeoutMs = options.awsRequestTimeoutMs;
  awsClientConfig_.connectTimeoutMs = options.awsConnectTimeoutMs;
  awsClientConfig_.maxConnections = options.awsMaxConnections;

  s3_client_ = Aws::S3::S3Client(
    awsClientCreds_,
    awsClientConfig_,
    options.awsPayloadSigningPolicy,
    false);
}

  if (followSymlinks_) {
    setRootDir(rootDir_);
  }
}

// TODO check S3 to make sure path exists
bool S3SourceQueue::setRootDir(const string &newRootDir) {
  WLOG(INFO) << "Setting Root dir: " << rootDir_;
  rootDir_.assign(newRootDir);
  return true;
}

S3SourceQueue::~S3SourceQueue() {
  // need to remove all the sources because they access metadata at the
  // destructor.
  clearSourceQueue();
  for (SourceMetaData *fileData : sharedFileData_) {
    delete fileData;
  }
  //The AWS SDK must be shut down by calling Aws::ShutdownAPI
  Aws::ShutdownAPI(aws_options_);
}

bool S3SourceQueue::buildQueueSynchronously() {
  auto startTime = Clock::now();
  WVLOG(1) << "buildQueueSynchronously() called";
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (initCalled_) {
      return false;
    }
    initCalled_ = true;
  }
  bool res = false;
  // either traverse bucket objects or we already have a fixed set of candidate
  // files
  if (exploreDirectory_) {
    res = explore();
  } else {
    WLOG(INFO) << "Using list of file info. Number of files "
               << fileInfo_.size();
    res = enqueueFiles();
  }
  {
    std::lock_guard<std::mutex> lock(mutex_);
    initFinished_ = true;
    enqueueFilesToBeDeleted();
    // TODO: comment why
    if (sourceQueue_.empty()) {
      conditionNotEmpty_.notify_all();
    }
  }
  directoryTime_ = durationSeconds(Clock::now() - startTime);
  WVLOG(1) << "finished initialization of S3SourceQueue in "
           << directoryTime_;
  return res;
}

// Not needed for S3
string S3SourceQueue::resolvePath(const string &path) {
  return path;
}

bool S3SourceQueue::explore() {
  WLOG(INFO) << "Exploring root dir " << rootDir_
             << " include_pattern : " << includePattern_
             << " exclude_pattern : " << excludePattern_
             << " prune_dir_pattern : " << pruneDirPattern_;
  WDT_CHECK(!rootDir_.empty());
  bool hasError = false;
  std::set<string> visited;
  std::regex includeRegex(includePattern_);
  std::regex excludeRegex(excludePattern_);
  std::regex pruneDirRegex(pruneDirPattern_);
  std::deque<string> todoList;
  todoList.push_back("");

  Aws::S3::Model::ListObjectsRequest objects_request;
  objects_request.WithBucket(options.awsBucket);

  auto list_objects_outcome = s3_client_.ListObjects(objects_request);

  if (list_objects_outcome.IsSuccess()) {

    Aws::Vector<Aws::S3::Model::Object> object_list =
      list_objects_outcome.GetResult().GetContents();

    for (auto const &s3_object : object_list) {
      if (threadCtx_->getAbortChecker()->shouldAbort()) {
        WLOG(ERROR) << "Directory transfer thread aborted";
        hasError = true;
        break;
      }

      if (!excludePattern_.empty() &&
          std::regex_match(s3_object.GetKey(), excludeRegex)) {
          continue;
      }
      if (!includePattern_.empty() &&
          !std::regex_match(s3_object.GetKey(), includeRegex)) {
          continue;
      }

      // not setting the filesize here as it may not actually be the file size
      WdtFileInfo fileInfo(
          s3_object.GetKey(),
          s3_object.GetETag());

      createIntoQueue(s3_object.GetKey(), fileInfo);
    }

  }else{
    hasError = true;
    WLOG(ERROR) << "ListObjects error: " <<
    list_objects_outcome.GetError().GetExceptionName() << " " <<
    list_objects_outcome.GetError().GetMessage();
  }


  WLOG(INFO) << "Number of files explored: " << numEntries_ << " opened "
             << numFilesOpened_ << " with direct " << numFilesOpenedWithDirect_
             << " errors " << std::boolalpha << hasError;

  return !hasError;
}

void S3SourceQueue::createIntoQueue(const string &fullPath,
                                           WdtFileInfo &fileInfo) {

  if (head_outcome.IsSuccess()) {

    Aws::S3::Model::HeadObjectRequest head_request;
    head_request.WithBucket(bucket_name);
    head_request.WithKey(s3_object.GetKey());
    auto head_outcome = s3_client_.HeadObject(head_request);
    object_header = head_outcome.GetResult();
    WVLOG(2) << "Found file " << s3_object.GetKey() << ":"
            << s3_object.GetETag() << " of size: "
            << object_header.GetContentLength();


      if (head_outcome.IsSuccess()) {
          //setting the size from the header as thats the size of the file
          fileinfo.fileSize = object_header.GetContentLength();
          fileinfo.Etag = object_header.GetETag();
      } else {
        hasError = true;
        std::cout << "ListHeader error: " <<
        head_outcome.GetError().GetExceptionName() << " " <<
        head_outcome.GetError().GetMessage();
        // TODO Set object as error
        continue
      }
  // TODO: currently we are treating small files(size less than blocksize) as
  // blocks. Also, we transfer file name in the header for all the blocks for a
  // large file. This can be optimized as follows -
  // a) if filesize < blocksize, we do not send blocksize and offset in the
  // header. This should be useful for tiny files(0-few hundred bytes). We will
  // have to use separate header format and commands for files and blocks.
  // b) if filesize > blocksize, we can use send filename only in the first
  // block and use a shorter header for subsequent blocks. Also, we can remove
  // block size once negotiated, since blocksize is sort of fixed.
  fileInfo.verifyAndFixFlags();
  SourceMetaData *metadata = new SourceMetaData();
  metadata->fullPath = fullPath;
  metadata->relPath = fileInfo.fileName;
  metadata->fd = fileInfo.fd;
  metadata->directReads = fileInfo.directReads;
  metadata->size = fileInfo.fileSize;
  if ((openFilesDuringDiscovery_ != 0) && (metadata->fd < 0)) {
    metadata->fd =
        FileUtil::openForRead(*threadCtx_, fullPath, metadata->directReads);
    ++numFilesOpened_;
    if (metadata->directReads) {
      ++numFilesOpenedWithDirect_;
    }
    metadata->needToClose = (metadata->fd >= 0);
    // works for -1 up to 4B files
    if (--openFilesDuringDiscovery_ == 0) {
      WLOG(WARNING) << "Already opened " << numFilesOpened_
                    << " files, will open the reminder as they are sent";
    }
  }
  std::unique_lock<std::mutex> lock(mutex_);
  sharedFileData_.emplace_back(metadata);
  createIntoQueueInternal(metadata);
}

void S3SourceQueue::createIntoQueueInternal(SourceMetaData *metadata) {
  // TODO: currently we are treating small files(size less than blocksize) as
  // blocks. Also, we transfer file name in the header for all the blocks for a
  // large file. This can be optimized as follows -
  // a) if filesize < blocksize, we do not send blocksize and offset in the
  // header. This should be useful for tiny files(0-few hundred bytes). We will
  // have to use separate header format and commands for files and blocks.
  // b) if filesize > blocksize, we can use send filename only in the first
  // block and use a shorter header for subsequent blocks. Also, we can remove
  // block size once negotiated, since blocksize is sort of fixed.
  auto &fileSize = metadata->size;
  auto &relPath = metadata->relPath;
  int64_t blockSizeBytes = blockSizeMbytes_ * 1024 * 1024;
  bool enableBlockTransfer = blockSizeBytes > 0;
  if (!enableBlockTransfer) {
    WVLOG(2) << "Block transfer disabled for this transfer";
  }
  // if block transfer is disabled, treating fileSize as block size. This
  // ensures that we create a single block
  auto blockSize = enableBlockTransfer ? blockSizeBytes : fileSize;
  std::vector<Interval> remainingChunks;
  int64_t seqId;
  FileAllocationStatus allocationStatus;
  int64_t prevSeqId = 0;
  auto it = previouslyTransferredChunks_.find(relPath);
  if (it == previouslyTransferredChunks_.end()) {
    // No previously transferred chunks
    remainingChunks.emplace_back(0, fileSize);
    seqId = nextSeqId_++;
    allocationStatus = NOT_EXISTS;
  } else if (it->second.getFileSize() > fileSize) {
    // file size is greater on the receiver side
    remainingChunks.emplace_back(0, fileSize);
    seqId = nextSeqId_++;
    WLOG(INFO) << "File size is greater in the receiver side " << relPath << " "
               << fileSize << " " << it->second.getFileSize();
    allocationStatus = EXISTS_TOO_LARGE;
    prevSeqId = it->second.getSeqId();
  } else {
    auto &fileChunksInfo = it->second;
    // Some portion of the file was sent in previous transfers. Receiver sends
    // the list of chunks to the sender. Adding all the bytes of those chunks
    // should give us the number of bytes saved due to incremental download
    previouslySentBytes_ += fileChunksInfo.getTotalChunkSize();
    remainingChunks = fileChunksInfo.getRemainingChunks(fileSize);
    if (remainingChunks.empty()) {
      WLOG(INFO) << relPath << " completely sent in previous transfer";
      return;
    }
    seqId = fileChunksInfo.getSeqId();
    allocationStatus = it->second.getFileSize() < fileSize
                           ? EXISTS_TOO_SMALL
                           : EXISTS_CORRECT_SIZE;
  }
  metadata->seqId = seqId;
  metadata->prevSeqId = prevSeqId;
  metadata->allocationStatus = allocationStatus;

  int blockNumber = 0;
  for (const auto &chunk : remainingChunks) {
    int64_t offset = chunk.start_;
    int64_t remainingBytes = chunk.size();
    int64_t blockTotal;
    if (remainingBytes <= blockSize) {
      blockTotal = 1;
    } else {
      blockTotal = (remainingBytes / blockSize);
      int64_t remainder = (remainingBytes % blockSize);
      if (remainder > 0) {
        blockTotal++;
      }
    }
    do {
      const int64_t size = std::min<int64_t>(remainingBytes, blockSize);
      std::unique_ptr<ByteSource> source = std::make_unique<FileByteSource>(
          metadata, size, offset, blockNumber, blockTotal);
      sourceQueue_.push(std::move(source));
      remainingBytes -= size;
      offset += size;
      blockNumber++;
    } while (remainingBytes > 0);
    totalFileSize_ += chunk.size();
  }
  numEntries_++;
  numBlocks_ += blockNumber;
  smartNotify(blockNumber);
}

bool S3SourceQueue::enqueueFiles() {
  for (auto &info : fileInfo_) {
    if (threadCtx_->getAbortChecker()->shouldAbort()) {
      WLOG(ERROR) << "Directory transfer thread aborted";
      return false;
    }
    string fullPath = rootDir_ + info.fileName;
    if (info.fileSize < 0) {
      struct stat fileStat;
      if (stat(fullPath.c_str(), &fileStat) != 0) {
        WPLOG(ERROR) << "stat failed on path " << fullPath;

        TransferStats failedSourceStat(info.fileName);
        failedSourceStat.setLocalErrorCode(BYTE_SOURCE_READ_ERROR);
        {
          std::unique_lock<std::mutex> lock(mutex_);
          failedSourceStats_.emplace_back(std::move(failedSourceStat));
        }

        return false;
      }
      info.fileSize = fileStat.st_size;
    }
    createIntoQueue(fullPath, info);
  }
  return true;
}

void S3SourceQueue::enqueueFilesToBeDeleted() {
  if (!deleteFiles_) {
    return;
  }
  if (!initFinished_ || previouslyTransferredChunks_.empty()) {
    // if the directory transfer has not finished yet or existing files list has
    // not yet been received, return
    return;
  }
  std::set<std::string> discoveredFiles;
  for (const SourceMetaData *metadata : sharedFileData_) {
    discoveredFiles.insert(metadata->relPath);
  }
  int64_t numFilesToBeDeleted = 0;
  for (auto &it : previouslyTransferredChunks_) {
    const std::string &fileName = it.first;
    if (discoveredFiles.find(fileName) != discoveredFiles.end()) {
      continue;
    }
    int64_t seqId = it.second.getSeqId();
    // extra file on the receiver side
    WLOG(INFO) << "Extra file " << fileName << " seq-id " << seqId
               << " on the receiver side, will be deleted";
    SourceMetaData *metadata = new SourceMetaData();
    metadata->relPath = fileName;
    metadata->size = 0;
    // we can reuse the previous seq-id
    metadata->seqId = seqId;
    metadata->allocationStatus = TO_BE_DELETED;
    sharedFileData_.emplace_back(metadata);
    // create a byte source with size and offset equal to 0
    std::unique_ptr<ByteSource> source =
        std::make_unique<FileByteSource>(metadata, 0, 0, 0, 0);
    sourceQueue_.push(std::move(source));
    numFilesToBeDeleted++;
  }
  numEntries_ += numFilesToBeDeleted;
  numBlocks_ += numFilesToBeDeleted;
  smartNotify(numFilesToBeDeleted);
}
}  // namespace datamover
