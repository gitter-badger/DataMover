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
#include <unordered_map>
#include <memory>

#include <datamover/endpoints/s3/S3Writer.h>
#include <datamover/util/CommonImpl.h>

#include <fcntl.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <sys/types.h>

#include <aws/core/utils/memory/stl/AWSStringStream.h>


namespace datamover {

S3Writer::~S3Writer() {
  // Make sure that the file is closed but this should be a no-op as the
  // caller should always call sync() and close() manually to check the error
  // code.
  //if (!isClosed()) {
  //  WLOG(ERROR ) << "File " << source_.getIdentifier()
  //               << " was not closed and needed to be closed in the S3Writer";
  //  close();
  //}
}

bool S3Writer::open() {
  auto &options = threadCtx_.getOptions();
  if (options.skip_writes) {
    return OK;
  }

  auto object = moverParent_->awsObjectTracker_.find(source_.getIdentifier());
  //AwsObject*& = object = moverParent_->awsObjectTracker_[source_.getIdentifier()];
  //if (object == 0) {
  if (object == moverParent_->awsObjectTracker_.end()) {
      //const AwsObject &newObject(source_.getBlockTotal());
      const int64_t totalBlocks = source_.getBlockTotal();
      const std::string key = source_.getIdentifier();
      //moverParent_->awsObjectTracker_.insert(std::make_pair(source_.getIdentifier(), newObject));
      moverParent_->awsObjectTracker_[key] = new AwsObject(totalBlocks);
  }

  std::unique_lock<std::mutex> lock(moverParent_->awsObjectTracker_[source_.getIdentifier()]->awsObjectMutex_);

  if(!moverParent_->awsObjectTracker_[source_.getIdentifier()]->isStarted()){
    Aws::S3::Model::CreateMultipartUploadRequest request;
    request.SetBucket(options.awsBucket);
    request.SetKey(source_.getIdentifier().c_str());

    auto response = moverParent_->s3_client_.CreateMultipartUpload(request);
    if (response.IsSuccess()) {
      Aws::S3::Model::CreateMultipartUploadResult result = response.GetResult();
      moverParent_->awsObjectTracker_[source_.getIdentifier()]->setMultipartKey(result.GetUploadId());
      moverParent_->awsObjectTracker_[source_.getIdentifier()]->markStarted();
    }else{
      auto error = response.GetError();
    }
    return response.IsSuccess();
  }
  // FIXME
  return true;
}

bool S3Writer::isClosed(){
    return moverParent_->awsObjectTracker_[source_.getIdentifier()]->isClosed();
}

bool S3Writer::close() {
  auto &options = threadCtx_.getOptions();
  if (options.skip_writes) {
    return OK;
  }
  auto object = moverParent_->awsObjectTracker_.find(source_.getIdentifier());
  if (object == moverParent_->awsObjectTracker_.end()) {
      // This should not be posible
      return false;
  }

  //if(moverParent_->awsObjectTracker_[source_.getIdentifier()]->isFinished()){
  if(moverParent_->awsObjectTracker_[source_.getIdentifier()]->getPartsLeft() == 0 && !isClosed()){
    std::unique_lock<std::mutex> lock(moverParent_->awsObjectTracker_[source_.getIdentifier()]->awsObjectMutex_);

    Aws::S3::Model::CompletedMultipartUpload completed;

    for(int i=0; i < source_.getBlockTotal(); i++){
        Aws::S3::Model::CompletedPart completedPart;

        completedPart.SetPartNumber(i + 1);
        completedPart.SetETag(moverParent_->awsObjectTracker_[source_.getIdentifier()]->getPartEtag(i));

        completed.AddParts(completedPart);
    }

    Aws::S3::Model::CompleteMultipartUploadRequest request;
    request.SetBucket(options.awsBucket);
    request.SetKey(source_.getIdentifier().c_str());
    request.SetUploadId(moverParent_->awsObjectTracker_[source_.getIdentifier()]->getMultipartKey());
    request.WithMultipartUpload(completed);

    auto response = moverParent_->s3_client_.CompleteMultipartUpload(request);
    if (response.IsSuccess()) {
      moverParent_->awsObjectTracker_[source_.getIdentifier()]->markClosed();
    }else{
      auto error = response.GetError();
    }
    return response.IsSuccess();
  }
  return true;
}

bool S3Writer::write(char *buffer, int64_t size) {
  auto &options = threadCtx_.getOptions();
  if (options.skip_writes) {
    return OK;
  }
  auto object = moverParent_->awsObjectTracker_.find(source_.getIdentifier());
  if (object == moverParent_->awsObjectTracker_.end()) {
      // This should not be posible
      return false;
  }

    auto data = Aws::MakeShared<Aws::StringStream>("PutObjectInputStream", std::stringstream::in | std::stringstream::out | std::stringstream::binary);
    data->write(buffer, size);

    Aws::S3::Model::UploadPartRequest request;
    request.SetBucket(options.awsBucket);
    request.SetKey(source_.getIdentifier().c_str());
    request.SetUploadId(moverParent_->awsObjectTracker_[source_.getIdentifier()]->getMultipartKey());
    request.SetPartNumber(source_.getBlockNumber() + 1);
    request.SetContentLength(size);
    request.SetBody(data);

    auto response = moverParent_->s3_client_.UploadPart(request);

    if (response.IsSuccess()) {
      Aws::S3::Model::UploadPartResult result = response.GetResult();
      moverParent_->awsObjectTracker_[source_.getIdentifier()]->markPartUploaded(source_.getBlockNumber(), result.GetETag());
      totalWritten_ += size;
    }else{
      auto error = response.GetError();
    }

  return true;
}

}
