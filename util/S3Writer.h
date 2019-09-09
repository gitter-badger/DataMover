/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#pragma once

#include <wdt/WdtConfig.h>
#include <wdt/util/Writer.h>

#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/CreateMultipartUploadResult.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/CompleteMultipartUploadResult.h>
#include <aws/s3/model/UploadPartRequest.h>
#include <aws/s3/model/UploadPartResult.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/auth/AWSCredentialsProvider.h>

namespace facebook {
namespace wdt {

class S3Writer {
 public:
  S3Writer(ThreadCtx &threadCtx, FileByteSource const *source)
      : threadCtx_(threadCtx),
        source_(source){
    Aws::SDKOptions options;
    Aws::InitAPI(options);
  }

  ~S3Writer();

  ErrorCode open();

  ErrorCode write(char *buf, int64_t size, int partNumber);

  int64_t getTotalWritten(){
    return totalWritten_;
  }

  ErrorCode close();

 private:

  /**
   * Return true if the file is already closed.
   */
  bool isClosed();

  ThreadCtx &threadCtx_;

  /// details of the source
  FileByteSource const *source_;

  /// number of bytes written
  int64_t totalWritten_{0};



};
}
}
