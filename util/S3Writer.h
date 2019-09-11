/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#pragma once

#include <list>
#include <string>

#include <wdt/util/CommonImpl.h>
#include <wdt/ByteSource.h>
#include <wdt/WdtConfig.h>
//#include <wdt/movers/FileS3.h>

#include <aws/core/Aws.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/CreateMultipartUploadResult.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/CompleteMultipartUploadResult.h>
#include <aws/s3/model/UploadPartRequest.h>
#include <aws/s3/model/UploadPartResult.h>
#include <aws/core/utils/StringUtils.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/auth/AWSCredentialsProvider.h>


namespace facebook {
namespace wdt {

class FileS3;

class AwsObject {
  public:
    explicit AwsObject(int partNumber, int partTotal) :
        partNumber_(partNumber),
        partTotal_(partTotal),
        partsLeft_(partTotal),
        partsStatus_(partTotal, false)
      {
    }
    AwsObject(){}

    void markPartUploaded(int partNumber){
        partsStatus_.assign(partNumber, true);
        partsLeft_--;
    }

    void markStarted(){
        uploadStarted_ = true;
    }

    void markClosed(){
        isClosed_ = true;
    }

    bool isStarted(){
        return uploadStarted_;
    }

    void setMultipartKey(Aws::String multipartKey){
        multipartKey_ = multipartKey;
    }

    Aws::String getMultipartKey(){
        return multipartKey_;
    }

    bool isFinished(){
        return !(bool)partsLeft_;
    }

    bool isClosed(){
        return isClosed_;
    }

   private:

    bool uploadStarted_{false};

    int partNumber_;
    int partTotal_;
    int partsLeft_;
    bool isClosed_{false};

    std::list<bool> partsStatus_;

    Aws::String multipartKey_{""};

  };

  typedef std::unordered_map<std::string, AwsObject> AwsObjectTrackerType;


class S3Writer {
 public:
  S3Writer(ThreadCtx &threadCtx,
           ByteSource &source,
           FileS3 *moverParent
           ) :
      threadCtx_(threadCtx),
      source_(source),
      moverParent_(moverParent) {
  }

  ~S3Writer();

  bool open();

  bool write(char *buf, int64_t size);

  int64_t getTotalWritten(){
    return totalWritten_;
  }

  bool close();


  //facebook::wdt::AwsObject activeObject_;

 private:

  /**
   * Return true if the file is already closed.
   */
  bool isClosed();


  /// number of bytes written
  int64_t totalWritten_{0};
  // should a be private but having issues witht he contructor initialization list.
  //

  // FIXME
  FileS3 *moverParent_;
  ThreadCtx &threadCtx_;
  ByteSource &source_;

  AwsObject activeObject_;


};


}
}
