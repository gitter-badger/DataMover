/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include <list>
#include <string>

#include <wdt/util/S3Writer.h>
#include <wdt/util/CommonImpl.h>
#include <wdt/movers/FileS3.h>

#include <fcntl.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <sys/types.h>


namespace facebook {
namespace wdt {

S3Writer::~S3Writer() {
  // Make sure that the file is closed but this should be a no-op as the
  // caller should always call sync() and close() manually to check the error
  // code.
  if (!isClosed()) {
    WLOG(ERROR ) << "File " << source_.getIdentifier()
                 << " was not closed and needed to be closed in the S3Writer";
    close();
  }
}

bool S3Writer::open() {
  auto &options = threadCtx_.getOptions();
  if (options.skip_writes) {
    return OK;
  }
  //std::lock_guard<std::mutex> lock(awsObjectMutex_);

  auto object = moverParent_->awsObjectTracker_.find(source_.getIdentifier());
  if (object == moverParent_->awsObjectTracker_.end()) {
      activeObject_ = AwsObject(source_.getBlockNumber(), source_.getBlockTotal());
      moverParent_->awsObjectTracker_.emplace(source_.getIdentifier(), activeObject_);
  }else{
      activeObject_ = moverParent_->awsObjectTracker_.at(source_.getIdentifier());
  }

  if(!activeObject_.isStarted()){
    Aws::S3::Model::CreateMultipartUploadRequest request;
    request.SetBucket(options.awsBucket);
    request.SetKey(source_.getIdentifier().c_str());

    auto response = moverParent_->s3_client_.CreateMultipartUpload(request);
    if (response.IsSuccess()) {
      activeObject_.markStarted();
    }else{
      auto error = response.GetError();
      WLOG(ERROR) << "AWS Create Multipart Upload ERROR: " << error.GetExceptionName() << " -- "
          << error.GetMessage() << " -- " << request.GetBucket();
    }
    return response.IsSuccess();
  }
  // FIXME
  return true;
}

bool S3Writer::isClosed(){
    activeObject_.isClosed();
}

bool S3Writer::close() {
  auto &options = threadCtx_.getOptions();
  if (options.skip_writes) {
    return OK;
  }
  //std::lock_guard<std::mutex> lock(awsObjectMutex_);

    Aws::S3::Model::CompletedMultipartUpload completed;
    Aws::S3::Model::CompletedPart completedPart;
    completedPart.SetPartNumber(source_.getBlockNumber());
    //completedPart.SetETag(result.GetETag());
    Aws::Vector<Aws::S3::Model::CompletedPart> completed_parts = {completedPart};
    completed.SetParts(completed_parts);

    Aws::S3::Model::CompleteMultipartUploadRequest request;
    request.SetBucket(options.awsBucket);
    request.SetKey(source_.getIdentifier().c_str());
    request.SetUploadId(activeObject_.getMultipartKey());
    request.SetMultipartUpload(completed);

    auto response = moverParent_->s3_client_.CompleteMultipartUpload(request);
    if (response.IsSuccess()) {
      activeObject_.markClosed();
    }else{
      auto error = response.GetError();
      WLOG(ERROR) << "AWS Create Multipart Upload ERROR: " << error.GetExceptionName() << " -- "
          << error.GetMessage() << " -- " << request.GetBucket();
    }
    return response.IsSuccess();
  return OK;
}

bool S3Writer::write(char *buffer, int64_t size) {
  auto &options = threadCtx_.getOptions();
  if (options.skip_writes) {
    return OK;
  }
  // Dont need a mutex lock as the dirqueue should never give
  // The same file block to more then one thread

  // TODO: do we care if this part was already marked as uploaded? I think so
    const std::shared_ptr<Aws::IOStream> request_body =
        Aws::MakeShared<Aws::StringStream>("");

    *request_body << buffer;

    Aws::S3::Model::UploadPartRequest request;
    request.SetBucket(options.awsBucket);
    request.SetKey(source_.getIdentifier().c_str());
    request.SetUploadId(activeObject_.getMultipartKey());
    request.SetPartNumber(source_.getBlockNumber());
    request.SetBody(request_body);

    auto response = moverParent_->s3_client_.UploadPart(request);

    if (response.IsSuccess()) {
      activeObject_.markPartUploaded(source_.getBlockNumber());
      totalWritten_ += size;
    }else{
      auto error = response.GetError();
      WLOG(ERROR) << "AWS Create Multipart Upload ERROR: " << error.GetExceptionName() << " -- "
          << error.GetMessage() << " -- " << request.GetBucket();
    }

  return true;
}

}
}
