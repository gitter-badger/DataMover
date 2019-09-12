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
#include <memory>

#include <wdt/util/S3Writer.h>
#include <wdt/util/CommonImpl.h>
#include <wdt/movers/FileS3.h>

#include <fcntl.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <sys/types.h>

#include <aws/core/utils/memory/stl/AWSStringStream.h>


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
  std::lock_guard<std::mutex> lock(moverParent_->awsObjectMutex_);
  WLOG(INFO) << "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!";
  WLOG(INFO) << "Opening: " << source_.getIdentifier();
  WLOG(INFO) << "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!";
  auto object = moverParent_->awsObjectTracker_.find(source_.getIdentifier());
  if (object == moverParent_->awsObjectTracker_.end()) {
      WLOG(INFO) << "CREATING NEW FOR: " << source_.getIdentifier();source_.getIdentifier();
      WLOG(INFO) << "Block Number: " << source_.getBlockNumber();
      WLOG(INFO) << "Total BLocks: " << source_.getBlockTotal();
      AwsObject activeObject_(source_.getBlockNumber(), source_.getBlockTotal());
      moverParent_->awsObjectTracker_.insert(source_.getIdentifier(), activeObject_);
  }else{
      AwsObject activeObject_ = moverParent_->awsObjectTracker_.at(source_.getIdentifier());
  }
  WLOG(INFO) << activeObject_.isFinished();
  WLOG(INFO) << activeObject_.isStarted();
  if(!activeObject_.isStarted()){
    WLOG(INFO) << "Creating MPUR";
    Aws::S3::Model::CreateMultipartUploadRequest request;
    request.SetBucket(options.awsBucket);
    request.SetKey(source_.getIdentifier().c_str());

    auto response = moverParent_->s3_client_.CreateMultipartUpload(request);
    if (response.IsSuccess()) {
      Aws::S3::Model::CreateMultipartUploadResult result = response.GetResult();
      WLOG(INFO) << "Multipart key: " << result.GetUploadId();
      activeObject_.setMultipartKey(result.GetUploadId());
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
  std::lock_guard<std::mutex> lock(moverParent_->awsObjectMutex_);
  WLOG(INFO) << "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!";
  WLOG(INFO) << "Closing " << source_.getIdentifier();
  WLOG(INFO) << "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!";
  auto object = moverParent_->awsObjectTracker_.find(source_.getIdentifier());
  if (object != moverParent_->awsObjectTracker_.end()) {
      AwsObject activeObject_ = moverParent_->awsObjectTracker_.at(source_.getIdentifier());
  }else{
      // This should not be posible
      return false;
  }

  if(activeObject_.partsDone_ == source_.getBlockTotal()){
  //if(activeObject_.isFinished()){
    WLOG(INFO) << "All File parts Finished we are now closeing";

    Aws::S3::Model::CompletedMultipartUpload completed;

    WLOG(INFO) << "Real Total Parts: " << source_.getBlockTotal();
    for(int i=0; i < source_.getBlockTotal(); i++){
        WLOG(INFO) << "Processing part: "<< i;
        Aws::S3::Model::CompletedPart completedPart;

        WLOG(INFO) << "N: " << (i + 1) << "E: " << activeObject_.getPartEtag(i);
        completedPart.SetPartNumber(i + 1);
        completedPart.SetETag(activeObject_.getPartEtag(i));

        completed.AddParts(completedPart);
    }

    Aws::S3::Model::CompleteMultipartUploadRequest request;
    request.SetBucket(options.awsBucket);
    request.SetKey(source_.getIdentifier().c_str());
    WLOG(INFO) << "MPK: " << activeObject_.getMultipartKey();
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
  }
  return false;
}

bool S3Writer::write(char *buffer, int64_t size) {
  auto &options = threadCtx_.getOptions();
  if (options.skip_writes) {
    return OK;
  }
  WLOG(INFO) << "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!";
  WLOG(INFO) << "Wrighting: " << source_.getIdentifier();
  WLOG(INFO) << "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!";
  // Dont need a mutex lock as the dirqueue should never give
  // The same file block to more then one thread

  // TODO: do we care if this part was already marked as uploaded? I think so
  /*
    std::shared_ptr<Aws::StringStream> stream =
        Aws::MakeShared<Aws::StringStream>(activeObject_.getMultipartKey());

    stream->rdbuf()->pubsetbuf(static_cast<char*>(const_cast<void*>(buffer)),
                               size);
    stream->rdbuf()->pubseekpos(size);

    stream->seekg(0);
    */

    //Aws::S3::Model::PutObjectRequest object_request;
    //object_request.WithBucket(output_bucket).WithKey(key_name);
    auto data = Aws::MakeShared<Aws::StringStream>("PutObjectInputStream", std::stringstream::in | std::stringstream::out | std::stringstream::binary);
    //data->write(reinterpret_cast<char*>(buffer), size);
    data->write(buffer, size);

    //*request_body << buffer;

    Aws::S3::Model::UploadPartRequest request;
    request.SetBucket(options.awsBucket);
    request.SetKey(source_.getIdentifier().c_str());
    request.SetUploadId(activeObject_.getMultipartKey());
    request.SetPartNumber(source_.getBlockNumber() + 1);
    request.SetContentLength(size);
    request.SetBody(data);

    auto response = moverParent_->s3_client_.UploadPart(request);

    if (response.IsSuccess()) {
      Aws::S3::Model::UploadPartResult result = response.GetResult();
      WLOG(INFO) << "Multipart ETag: " << result.GetETag();
      activeObject_.markPartUploaded(source_.getBlockNumber(), result.GetETag());
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
