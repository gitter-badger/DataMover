/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#include <wdt/util/S3Writer.h>
#include <wdt/util/CommonImpl.h>

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
    WLOG(ERROR ) << "File " << blockDetails_->fileName
                 << " was not closed and needed to be closed in the dtor";
    close();
  }
}

ErrorCode S3Writer::open() {
  if (threadCtx_.getOptions().skip_writes) {
    return OK;
  }
  return OK;
}

ErrorCode S3Writer::close() {

  return OK;
}

bool S3Writer::isClosed() {
}

ErrorCode S3Writer::write(char *buf, int64_t size) {
  auto &options = threadCtx_.getOptions();
  if (options.skip_writes) {
    return OK;
  }
    const std::shared_ptr<Aws::IOStream> request_body =
        Aws::MakeShared<Aws::StringStream>("");

    *request_body << buffer;

  totalWritten_ += size;
  return OK;
}

ErrorCode S3Writer::writeObject(char *buf, int64_t size) {
}

ErrorCode S3Writer::startMultipartObject(char *buf, int64_t size) {
    Aws::S3::Model::CreateMultipartUploadRequest cmu_request;
    cmu_request.SetBucket(bucket); /// FIXME
    cmu_request.SetKey(blockDetails.fileName.c_str());

    Aws::S3::Model::CreateMultipartUploadOutcome cmu_response = s3_client.CreateMultipartUpload(cmu_request);

    if (!cmu_response.IsSuccess()) {
        auto error = cmu_response.GetError();
        WLOG(ERROR) << "cmu s3 ERROR: " << error.GetExceptionName() << " -- "
            << error.GetMessage();
        WLOG(INFO) << "bucketname: !" << cmu_request.GetBucket() << "!";
        threadStats_.setLocalErrorCode(ABORT);
        stats.incrFailedAttempts();
        return stats;
    }
}

ErrorCode S3Writer::writeMultipartObject(char *buf, int64_t size) {
    Aws::S3::Model::CreateMultipartUploadResult cmu_result;
    cmu_result = cmu_response.GetResult();
    WLOG(INFO) << "Upload ID: " << cmu_result.GetUploadId();

    WLOG(INFO) << "Block num: " << source->getBlockNum();
    Aws::S3::Model::UploadPartRequest request;
    request.SetBucket(bucket);
    request.SetKey(blockDetails.fileName.c_str());
    request.SetUploadId(cmu_result.GetUploadId());
    request.SetPartNumber(source->getBlockNum());
    request.SetBody(request_body);

    Aws::S3::Model::UploadPartOutcome response = s3_client.UploadPart(request);
    stats.addDataBytes(size);

    if (!response.IsSuccess()) {
        auto error = response.GetError();
        WLOG(ERROR) << "s3 ERROR: " << error.GetExceptionName() << " -- "
            << error.GetMessage();
        WLOG(INFO) << "bucketname: !" << request.GetBucket() << "!";
        threadStats_.setLocalErrorCode(ABORT);
        stats.incrFailedAttempts();
        // virtual Model::AbortMultipartUploadOutcome Aws::S3::S3Client::AbortMultipartUpload   (   const Model::AbortMultipartUploadRequest &     request )   const
        return stats;
    }

    Aws::S3::Model::UploadPartResult result;
    result = response.GetResult();
    WLOG(INFO) << "File Name: " << blockDetails.fileName << " MD5: " << result.GetETag();

}

ErrorCode S3Writer::finishMultipartObject(char *buf, int64_t size) {

    Aws::S3::Model::CompletedMultipartUpload completed;
    Aws::S3::Model::CompletedPart completedPart;
    completedPart.SetPartNumber(source->getBlockNum());
    completedPart.SetETag(result.GetETag());
    Aws::Vector<Aws::S3::Model::CompletedPart> completed_parts = {completedPart};
    completed.SetParts(completed_parts);

    Aws::S3::Model::CompleteMultipartUploadRequest complete_request;
    complete_request.SetBucket(bucket);
    complete_request.SetKey(blockDetails.fileName.c_str());
    complete_request.SetUploadId(cmu_result.GetUploadId());
    complete_request.SetMultipartUpload(completed);

    Aws::S3::Model::CompleteMultipartUploadOutcome complete_response = s3_client.CompleteMultipartUpload(complete_request);
    if (!complete_response.IsSuccess()) {
        auto error = complete_response.GetError();
        WLOG(ERROR) << "complete s3 ERROR: " << error.GetExceptionName() << " -- "
            << error.GetMessage();
        WLOG(INFO) << "bucketname: !" << request.GetBucket() << "!";
        threadStats_.setLocalErrorCode(ABORT);
        stats.incrFailedAttempts();
        // virtual Model::AbortMultipartUploadOutcome Aws::S3::S3Client::AbortMultipartUpload   (   const Model::AbortMultipartUploadRequest &     request )   const
        return stats;
    }
    WLOG(INFO) << "File Name: " << blockDetails.fileName << " MD5: " << result.GetETag();
}

}
}
