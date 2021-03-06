# CMake build file - use CMake 3.2 or later to build DM and its dependencies.
#
# mkdir build; cd build; cmake .. -DBUILD_TESTING=on; make -j
# omit -DBUILD_TESTING=on if you don't want the extra dependencies for
# testing (but testing is good !)
#
#  Copyright (c) 2014-present, Facebook, Inc.
#  All rights reserved.
#
#  This source code is licensed under the BSD-style license found in the
#  LICENSE file in the root directory of this source tree. An additional grant
#  of patent rights can be found in the PATENTS file in the same directory.
#

cmake_minimum_required(VERSION 3.2)

# There is no C per se in WDT but if you use CXX only here many checks fail
# Version is Major.Minor.YYMMDDX for up to 10 releases per day (X from 0 to 9)
# Minor currently is also the protocol version - has to match with Protocol.cpp
project("DataMover" LANGUAGES C CXX VERSION 1.31.1903011)

# On MacOS this requires the latest (master) CMake (and/or CMake 3.1.1/3.2)
# DM itself works fine with C++11 (gcc 4.8 for instance) but more recent folly
# started to require C++14, so you can either get an older snapshot of folly
# and set "11" below or use this:
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED on)

# somehow 'option' for this doesn't seeem to work/I don't know how to make it
set(BUILD_SHARED_LIBS on CACHE Bool "build shared libs")

set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# use, i.e. don't skip the full RPATH for the build tree
set(CMAKE_SKIP_BUILD_RPATH FALSE)

# when building, don't use the install RPATH already
# (but later on when installing)
set(CMAKE_BUILD_WITH_INSTALL_RPATH FALSE)

# the RPATH to be used when installing
set(CMAKE_INSTALL_RPATH "$ORIGIN")

# don't add the automatically determined parts of the RPATH
# which point to directories outside the build tree to the install RPATH
set(CMAKE_INSTALL_RPATH_USE_LINK_PATH FALSE)

set(CMAKE_LIBRARY_PATH "/var/tmp/dm_install/usr/local/lib64;/var/tmp/dm_install/usr/local/lib")
set(CMAKE_INCLUDE_PATH "/var/tmp/dm_install/usr/local/include")


# Optimized by default
# TODO: This doesn't seem to work / sets default to "" instead of Release...
# set(CMAKE_BUILD_TYPE Release CACHE String "build type")
# So hardcoding for now:
set(CMAKE_BUILD_TYPE Debug)
#set(CMAKE_BUILD_TYPE Release)
set(CMAKE_CXX_FLAGS " -msse4.2 -mpclmul")
#set(CMAKE_CXX_FLAGS "-Wall -ggdb -msse4.2 -mpclmul -fsanitize=address")
#set(CMAKE_CXX_FLAGS " -Wall -ggdb -msse4.2 -mpclmul")
#set(CMAKE_CXX_FLAGS "-msse4.2 -mpclmul -Wextra -Wsign-compare -Wunused-variable")
#set(CMAKE_CXX_FLAGS "-msse4.2 -mpclmul -Wextra -Wsign-compare -Wunused-variable -Wconversion -Wsign-conversion")
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "_bin/datamover")

# Check that we have the Folly source tree
set(FOLLY_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../folly" CACHE path
    "Folly source tree (folly/Conv.h should be reachable from there")
# Check for folly - TODO: this doesn't work well for relative paths
# (because of relative to build dir vs relative to source tree for -I)
if(NOT EXISTS "${FOLLY_SOURCE_DIR}/folly/Conv.h")
  MESSAGE(FATAL_ERROR "${FOLLY_SOURCE_DIR}/folly/Conv.h not found
Fix using:
(in a sister directory of the DM source tree - same level:)
git clone https://github.com/facebook/folly.git
or change FOLLY_SOURCE_DIR (use ccmake or -DFOLLY_SOURCE_DIR=...)
")
endif()


# The part of folly that isn't pure .h and we use:
set (FOLLY_CPP_SRC
    "${FOLLY_SOURCE_DIR}/folly/Conv.cpp"
    "${FOLLY_SOURCE_DIR}/folly/lang/CString.cpp"
    "${FOLLY_SOURCE_DIR}/folly/Demangle.cpp"
    "${FOLLY_SOURCE_DIR}/folly/hash/Checksum.cpp"
    "${FOLLY_SOURCE_DIR}/folly/hash/detail/ChecksumDetail.cpp"
    "${FOLLY_SOURCE_DIR}/folly/hash/detail/Crc32cDetail.cpp"
    "${FOLLY_SOURCE_DIR}/folly/hash/detail/Crc32CombineDetail.cpp"
    "${FOLLY_SOURCE_DIR}/folly/ScopeGuard.cpp"
)


# DataMover's library proper - comes from: ls -1 *.cpp | grep -iv test

add_library(datamover SHARED
    WdtTransferRequest.cpp
    WdtOptions.cpp
    util/CommonImpl.cpp
    util/ServerSocket.cpp
    util/Stats.cpp
    util/ClientSocket.cpp
    util/ThreadTransferHistory.cpp
    util/SerializationUtil.cpp
    util/WdtSocket.cpp
    util/ThreadsController.cpp
    util/EncryptionUtils.cpp
    util/TransferLogManager.cpp
    endpoints/SourceQueue.cpp
    endpoints/file/FileWriter.cpp
    endpoints/file/DirectorySourceQueue.cpp
    endpoints/file/FileByteSource.cpp
    endpoints/file/FileCreator.cpp
    Reporting.cpp
    Throttler.cpp
    ErrorCodes.cpp
    WdtThread.cpp
    movers/wdt/file/WdtFile.cpp
    movers/wdt/file/WdtFileThread.cpp
    movers/file/file/FileFileThread.cpp
    movers/file/file/FileFile.cpp
    movers/file/wdt/FileWdt.cpp
    movers/file/wdt/FileWdtThread.cpp
    movers/MoverBase.cpp
    Protocol.cpp
)

TARGET_LINK_LIBRARIES(datamover
    aws-cpp-sdk-core
    aws-c-event-stream
    aws-cpp-sdk-s3
    aws-c-common
    aws-checksums
)

# Source files that depend on gflags and provide flags -> options init
#set (DM_FLAGS_RELATED_SRC
#util/WdtFlags.cpp
#)
#add_library(datamover ${DM_FLAGS_RELATED_SRC})

target_link_libraries(datamover)



# Folly uses boost system and filesystem
#set(Boost_USE_STATIC_LIBS on)
find_package(Boost COMPONENTS system filesystem REQUIRED)
include_directories(${Boost_INCLUDE_DIRS})

# We use std:: threads
find_package(Threads) # this will set ${CMAKE_THREAD_LIBS_INIT} (ie pthreads)

# double-conversion
find_path(DOUBLECONV_INCLUDE_DIR double-conversion/double-conversion.h)
find_library(DOUBLECONV_LIBRARY double-conversion)

# Glog
find_path(GLOG_INCLUDE_DIR glog/logging.h)
find_library(GLOG_LIBRARY glog)

# Gflags
find_path(GFLAGS_INCLUDE_DIR gflags/gflags.h)
find_library(GFLAGS_LIBRARY gflags)

# OpenSSL's crypto lib}
find_package(OpenSSL REQUIRED)
include_directories(${OPENSSL_INCLUDE_DIR})

# You can also add jemalloc to the list if you have it/want it
target_link_libraries(datamover
  folly4datamover
  ${GLOG_LIBRARY}
  ${GFLAGS_LIBRARY}
  ${Boost_LIBRARIES}
  ${DOUBLECONV_LIBRARY}
  ${OPENSSL_CRYPTO_LIBRARY}
  ${CMAKE_THREAD_LIBS_INIT} # Must be last to avoid link errors
)

# What we need to build the part of folly we use:

include(CheckIncludeFileCXX)
include(CheckFunctionExists)
include(CheckLibraryExists)
include(CheckCXXSourceCompiles)
# For DataMover itself:
check_function_exists(posix_fallocate HAS_POSIX_FALLOCATE)
check_function_exists(sync_file_range HAS_SYNC_FILE_RANGE)
check_function_exists(posix_memalign HAS_POSIX_MEMALIGN)
check_function_exists(posix_fadvise HAS_POSIX_FADVISE)
# C based check (which fail with the c++ setting thereafter...)
check_function_exists(clock_gettime FOLLY_HAVE_CLOCK_GETTIME)
# was: check_library_exists(rt clock_gettime "" FOLLY_HAVE_CLOCK_GETTIME)
check_function_exists(memrchr FOLLY_HAVE_MEMRCHR)
set(SAVE_CMRL ${CMAKE_REQUIRED_LIBRARIES}) #globals are evil/ugly
set(CMAKE_REQUIRED_LIBRARIES ${CMAKE_THREAD_LIBS_INIT})
check_function_exists(pthread_atfork FOLLY_HAVE_PTHREAD_ATFORK)
set(CMAKE_REQUIRED_LIBRARIES ${SAVE_CMRL}) #globals are evil/ugly
# Needed until Cmake issue #15361 is addressed
# All the check_function_exists should be before this line
# or else they will not work'

set(CMAKE_REQUIRED_DEFINITIONS ${CMAKE_CXX11_STANDARD_COMPILE_OPTION})
check_include_file_cxx(linux/sockios.h DM_HAS_SOCKIOS_H)
#check_function_exists(clock_gettime FOLLY_HAVE_CLOCK_GETTIME)
check_cxx_source_compiles("#include <type_traits>
      #if !_LIBCPP_VERSION
      #error No libc++
      #endif
      int main() {return 0;}" FOLLY_USE_LIBCPP)
check_cxx_source_compiles(
"extern \"C\" void cmkcheckweak() __attribute__((weak));
int main(int argc, char** argv) {
  return (cmkcheckweak) ? 1 : 0 ;
}" FOLLY_HAVE_WEAK_SYMBOLS)
# Now record all this :
# Folly's:
configure_file(build/folly-config.h.in folly/folly-config.h)
# datamover's config/version
configure_file(WdtConfig.h.in wdt/WdtConfig.h)

# Malloc stuff  tied to not supporting weaksympbols
if (NOT FOLLY_HAVE_WEAK_SYMBOLS)
  list(APPEND FOLLY_CPP_SRC "${FOLLY_SOURCE_DIR}/folly/memory/detail/MallocImpl.cpp")
  message(STATUS "no weak symbols, adding MallocImpl to folly src")
endif()

add_library(folly4datamover ${FOLLY_CPP_SRC})
target_link_libraries(folly4datamover ${GLOG_LIBRARY} ${DOUBLECONV_LIBRARY})

# Order is important - inside fb we want the above
# folly-config.h to be picked up instead of the fbcode one
include_directories(${CMAKE_CURRENT_BINARY_DIR})
include_directories(${FOLLY_SOURCE_DIR})
include_directories(${DOUBLECONV_INCLUDE_DIR})
include_directories(${GLOG_INCLUDE_DIR})
include_directories(${GFLAGS_INCLUDE_DIR})
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/..)

set_target_properties(PROPERTIES COMPILE_DEFINITIONS "STANDALONE_APP")

target_link_libraries(datamover)

### Install rules
set_target_properties( PROPERTIES RUNTIME_OUTPUT_NAME "datamover")
install(TARGETS datamover folly4datamover
  RUNTIME DESTINATION bin
  LIBRARY DESTINATION lib64
  ARCHIVE DESTINATION lib64
  )

### Install header files

file(GLOB headers "*.h")
foreach(header ${headers})
  install(FILES ${header} DESTINATION include/datamover)
endforeach()


# Install the .h files in the util with directory structure maintained
set (FOLLY_HEADERS
    "/folly/synchronization/RWSpinLock.h"
    "/folly/Portability.h"
    "/folly/CPortability.h"
    "/folly/portability/Config.h"
    "/folly/portability/Asm.h"
    "/folly/portability/Malloc.h"
    "/folly/portability/Constexpr.h"
    "/folly/portability/String.h"
    "/folly/Likely.h"
    "/folly/Memory.h"
    "/folly/ConstexprMath.h"
    "/folly/Traits.h"
    "/folly/functional/Invoke.h"
    "/folly/Preprocessor.h"
    "/folly/lang/Align.h"
    "/folly/lang/Exception.h"
    "/folly/CppAttributes.h"
    "/folly/Range.h"
    "/folly/hash/SpookyHashV2.h"
    "/folly/CpuId.h"
    "/folly/detail/RangeCommon.h"
    "/folly/detail/RangeSse42.h"
    "/folly/SpinLock.h"
    "/folly/synchronization/SmallLocks.h"
    "/folly/MicroLock.h"
    "/folly/detail/Futex.h"
    "/folly/portability/Unistd.h"
    "/folly/detail/Futex-inl.h"
    "/folly/synchronization/ParkingLot.h"
    "/folly/Hash.h"
    "/folly/hash/Hash.h"
    "/folly/Utility.h"
    "/folly/functional/ApplyTuple.h"
    "/folly/hash/SpookyHashV1.h"
    "/folly/lang/Bits.h"
    )
foreach(header ${FOLLY_HEADERS})
    get_filename_component(header_dir "${header}" PATH)
    install(
        FILES "${FOLLY_SOURCE_DIR}${header}"
        DESTINATION "include/${header_dir}"
    )
endforeach()

install(
    FILES "${PROJECT_BINARY_DIR}/folly/folly-config.h"
    DESTINATION "include/folly/"
)

install(DIRECTORY "${PROJECT_SOURCE_DIR}/util" DESTINATION include/datamover
        FILES_MATCHING PATTERN "*.h")

install(DIRECTORY "${PROJECT_SOURCE_DIR}/movers" DESTINATION include/datamover
        FILES_MATCHING PATTERN "*.h")

install(DIRECTORY "${PROJECT_SOURCE_DIR}/endpoints" DESTINATION include/datamover
        FILES_MATCHING PATTERN "*.h")



### Everything below is about testing (and not required to create datamover)

if (BUILD_TESTING)

  enable_testing()

  # Extra code that we use in tests
  add_library(wdt4tests_min
    test/TestCommon.cpp
  )

  include(ExternalProject)

  # GTest
  set(GTEST_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/gtest")
  externalproject_add(
    gtest
    GIT_REPOSITORY https://github.com/google/googletest.git
    INSTALL_COMMAND "" # Disable install step
    UPDATE_COMMAND "" # Doesn't change often
    PREFIX "${GTEST_PREFIX}"
    #CONFIGURE_COMMAND "" # skip
    #BUILD_COMMAND "" # skip
    LOG_DOWNLOAD ON
    LOG_CONFIGURE ON
    LOG_BUILD ON
  )
  # Specify include dir for gtest
  externalproject_get_property(gtest SOURCE_DIR)
  include_directories("${SOURCE_DIR}/googletest/include")

  externalproject_get_property(gtest BINARY_DIR)

  #  add_library(gmock_all STATIC EXCLUDE_FROM_ALL
  #   ${GMOCK_PREFIX}/src/gmock/gtest/src/gtest-all.cc
  #   ${GMOCK_PREFIX}/src/gmock/gmock-all.cc
  #   ${GMOCK_PREFIX}/src/gmock/gmock_main.cc)

  add_dependencies(wdt4tests_min gtest)

  # ${BINARY_DIR}/libgtest.a works everywhere except xcode...
# so ugly weird hack generating warnings about unknown dir for now:
  target_link_libraries(wdt4tests_min
    #"-L ${BINARY_DIR}/googlemock/gtest -L ${BINARY_DIR}/Debug -lgtest"
    "-L ${BINARY_DIR}/googlemock/gtest -lgtest"
    wdt_min
  )

  add_library(wdt4tests
    ${WDT_FLAGS_RELATED_SRC}
  )
  target_link_libraries(wdt4tests wdt4tests_min)

  # TODO: make a macro/function to add tests in 1 line instead of 3


  # WDT testing/benchmarking code
  add_library(wdtbenchlib
    bench/Bigram.cpp
  )

  target_link_libraries(wdtbenchlib
    ${GLOG_LIBRARY}
    ${GFLAGS_LIBRARY}
  )

  add_library(wdtbenchtestslib
    bench/WdtGenTestUtils.cpp
  )
  add_dependencies(wdtbenchtestslib gtest)
  target_link_libraries(wdtbenchtestslib
    "-L ${BINARY_DIR}/googlemock/gtest -lgtest"
    wdtbenchlib
    ${CMAKE_THREAD_LIBS_INIT} # Must be last to avoid link errors
  )

  add_executable(wdt_gen_files bench/wdtGenFiles.cpp)
  target_link_libraries(wdt_gen_files wdtbenchlib
    ${CMAKE_THREAD_LIBS_INIT} # Must be last to avoid link errors
  )
  set_target_properties(wdt_gen_files PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "_bin/wdt/bench/")

  add_executable(wdt_gen_stats bench/wdtStats.cpp)
  target_link_libraries(wdt_gen_stats wdtbenchlib)

  add_executable(wdt_gen_test bench/wdtGenTest.cpp)
  target_link_libraries(wdt_gen_test wdtbenchtestslib)
  add_test(NAME AllTestsInGenTest COMMAND wdt_gen_test)

  # Regular tests

  add_executable(protocol_test test/ProtocolTest.cpp)
  target_link_libraries(protocol_test wdt4tests)
  add_test(NAME AllTestsInProtocolTest COMMAND protocol_test)

  add_executable(test_encdeci64_func test/test_encdeci64_func.cpp)
  target_link_libraries(test_encdeci64_func wdt4tests)
  add_test(NAME test_encdeci64_func COMMAND test_encdeci64_func)

  add_executable(test_stats test/Stats_test.cpp)
  target_link_libraries(test_stats wdt4tests)
  add_test(NAME test_stats COMMAND test_stats)

  add_executable(histogram test/Histogram.cpp)
  target_link_libraries(histogram wdt_min)

  add_executable(resource_controller_test  test/WdtResourceControllerTest.cpp)
  target_link_libraries(resource_controller_test wdt4tests)
  add_test(NAME ResourceControllerTests COMMAND resource_controller_test)

  add_executable(wdt_url_test  test/WdtUrlTest.cpp)
  target_link_libraries(wdt_url_test wdt4tests)
  add_test(NAME WdtUrlTests COMMAND wdt_url_test)

  add_executable(wdt_misc_tests  test/WdtMiscTests.cpp)
  target_link_libraries(wdt_misc_tests wdt4tests)
  add_test(NAME WdtMiscTests COMMAND wdt_misc_tests)

  add_executable(wdt_fd_test  test/FdTest.cpp)
  target_link_libraries(wdt_fd_test wdt4tests)
  add_test(NAME WdtFdTests COMMAND wdt_fd_test)

  add_executable(encryption_test  test/EncryptionTest.cpp)
  target_link_libraries(encryption_test wdt4tests)
  add_test(NAME EncryptionTests COMMAND encryption_test)

  add_executable(file_reader_test  test/FileReaderTest.cpp)
  target_link_libraries(file_reader_test wdt4tests)
  add_test(NAME FileReaderTests COMMAND file_reader_test)

  add_executable(option_type_test_long_flags test/OptionTypeTest.cpp)
  target_link_libraries(option_type_test_long_flags wdt4tests)

  add_executable(option_type_test_short_flags test/OptionTypeTest.cpp
                                              ${WDT_FLAGS_RELATED_SRC}
                                              )
  set_target_properties(option_type_test_short_flags PROPERTIES
    COMPILE_DEFINITIONS "STANDALONE_APP"
    RUNTIME_OUTPUT_DIRECTORY "_bin/wdt/short_flags/")

  target_link_libraries(option_type_test_short_flags wdt4tests_min)

  add_test(NAME WdtRandGenTest COMMAND
    "${CMAKE_CURRENT_SOURCE_DIR}/test/wdt_rand_gen_test.sh")

  add_test(NAME WdtBasicE2E COMMAND
    "${CMAKE_CURRENT_SOURCE_DIR}/test/wdt_e2e_simple_test.sh")

# Doesn't work on a mac:
#  add_test(NAME WdtStdinManifestAndUrl COMMAND
#    "${CMAKE_CURRENT_SOURCE_DIR}/test/wdt_stdin_test.sh")

  add_test(NAME WdtLockFailFast COMMAND
    "${CMAKE_CURRENT_SOURCE_DIR}/test/wdt_lock_failfast.sh")

  add_test(NAME WdtBasicE2Exfs COMMAND
    "${CMAKE_CURRENT_SOURCE_DIR}/test/wdt_e2e_xfs_test.sh")

  add_test(NAME WdtOptionsTypeTests COMMAND
    "${CMAKE_CURRENT_SOURCE_DIR}/test/wdt_option_type_test.sh")

  add_test(NAME hostname_override_test COMMAND
    "${CMAKE_CURRENT_SOURCE_DIR}/test/hostname_override_test.py")

  add_test(NAME WdtPortBlockTest COMMAND
    "${CMAKE_CURRENT_SOURCE_DIR}/test/wdt_port_block_test.py")

  add_test(NAME WdtProtocolNegotiationTest COMMAND
    "${CMAKE_CURRENT_SOURCE_DIR}/test/wdt_protocol_negotiation_test.py")

  add_test(NAME WdtSimpleOdirectTest COMMAND
    "${CMAKE_CURRENT_SOURCE_DIR}/test/wdt_e2e_simple_test.sh" -o true)

  add_test(NAME WdtFileListTest COMMAND
    "${CMAKE_CURRENT_SOURCE_DIR}/test/wdt_file_list_test.py")

  add_test(NAME WdtOverwriteTest COMMAND
    "${CMAKE_CURRENT_SOURCE_DIR}/test/wdt_overwrite_test.py")

  add_test(NAME WdtBadServerTest COMMAND
    "${CMAKE_CURRENT_SOURCE_DIR}/test/wdt_bad_server_test.py")

  add_test(NAME ReceiverThrottlerRefCountTest COMMAND
    "${CMAKE_CURRENT_SOURCE_DIR}/test/receiver_throttler_ref_count_test.sh")

  add_test(NAME WdtLongRunningTest COMMAND
    "${CMAKE_CURRENT_SOURCE_DIR}/test/wdt_long_running_test.py")

endif(BUILD_TESTING)
