/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#pragma once

#include <unordered_map>
#include <string>
#include <memory>

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
        partsLeft_(partTotal),
        partTotal_(partTotal) {}

    AwsObject(){};


    void markPartUploaded(int partNumber, Aws::String etag){
        partsStatus_[partNumber] = etag;
        partsLeft_--;
        partsDone_++;
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

    int getPartsLeft(){
        return partsLeft_;
    }

    Aws::String getPartEtag(int partNumber){
        auto object = partsStatus_.find(partNumber);
        if (object != partsStatus_.end()) {
            return partsStatus_[partNumber];
        }
        // FIXME should never happen should error out
        return "";
    }

    void setMultipartKey(Aws::String multipartKey){
        multipartKey_ = multipartKey;
    }

    Aws::String getMultipartKey(){
        return multipartKey_;
    }

    bool isFinished(){
        if(partsLeft_ > 0){
            return false;
        }
        return true;
    }

    bool isClosed(){
        return isClosed_;
    }

    //std::mutex activeMutex_;

   private:

    // Mutex lock to properly set upload multipart files
    std::mutex awsObjectMutex_;

    bool uploadStarted_{false};

    bool isClosed_{false};

    std::unordered_map<int, Aws::String> partsStatus_;

    Aws::String multipartKey_{""};

    // FIXME
    int partNumber_{0};
    int partsLeft_{0};
    int partTotal_{0};
    int partsDone_{0};

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


 private:

  bool isClosed();

  /// number of bytes written
  int64_t totalWritten_{0};


  // FIXME
  FileS3 *moverParent_;
  ThreadCtx &threadCtx_;
  ByteSource &source_;

  AwsObject activeObject_;


};


}
}
