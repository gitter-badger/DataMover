#!/bin/bash

# manually install the following via yum/apt/pacman(with .h files)...
#    - boost 1.6.9
#    - libdouble-conversion 3.1.5
#    - cmake 3.2+
#    - automake
#    - libtool

BUILD_DIR=/var/tmp/dm_build
INST_DIR=/var/tmp/dm_install
SRC_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )/src/datamover/"

if true; then

mkdir -p $BUILD_DIR
rm -Rf $BUILD_DIR
mkdir -p $BUILD_DIR
cd $BUILD_DIR

##################################
## folly
##################################
git clone https://github.com/facebook/folly.git

##################################
## gflags
##################################
git clone https://github.com/schuhschuh/gflags.git
mkdir gflags/build
cd gflags/build
rm ${BUILD_DIR}/DataMover/src/datamover/CMakeCache.txt
cmake \
    -DGFLAGS_NAMESPACE=google \
    -DBUILD_SHARED_LIBS=on \
    -DCMAKE_INSTALL_PREFIX:PATH=${INST_DIR}/usr/local/ \
    ..
make
make install
cd $BUILD_DIR

##################################
## glogs
##################################
git clone https://github.com/google/glog.git
cd glog
mkdir build
cd build
#./autogen.sh
#./configure --with-gflags=${INST_DIR}/usr/local/
#make -j && sudo make install

cmake ..  \
    -G "Unix Makefiles" \
    -DWITH_GFLAGS=OFF \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_PREFIX_PATH="${INST_DIR}/usr/local/" \
    -DCMAKE_INSTALL_PREFIX="${INST_DIR}/usr/local/" \
    -DCMAKE_INSTALL_LIBDIR=lib64 \
    -DINSTALL_HEADERS=1 \
    -DBUILD_SHARED_LIBS=1 \
    -DBUILD_STATIC_LIBS=0 \
    -DBUILD_TESTING=0
make
make install
cd $BUILD_DIR

##################################
## Double conversion
##################################
git clone https://github.com/floitsch/double-conversion.git
cd double-conversion;
mkdir build
cd build
rm ${BUILD_DIR}/DataMover/src/datamover/CMakeCache.txt
cmake \
    .. \
    -DCMAKE_PREFIX_PATH="${INST_DIR}/usr/local/" \
    -DCMAKE_INSTALL_PREFIX="${INST_DIR}/usr/local/" \
    -DBUILD_SHARED_LIBS=on

make -j
#make install
cd $BUILD_DIR

##################################
## DataMover
##################################

mkdir -p dm_build
rm -Rf dm_build
mkdir -p dm_build

fi

cp -r ${HOME}/git/DataMover ./
#ln -s DataMover wdt


cd ${BUILD_DIR}/dm_build
rm ${BUILD_DIR}/DataMover/src/datamover/CMakeCache.txt

cmake3 \
    ${BUILD_DIR}/DataMover/src/datamover \
    -DBUILD_TESTING=off \
    -DFOLLY_SOURCE_DIR=${BUILD_DIR}/folly \
    -DBOOST_INCLUDEDIR=/usr/include/boost169 \
    -DBOOST_LIBRARYDIR=/usr/lib64/boost169 \
    -DCMAKE_INSTALL_PREFIX:PATH=${INST_DIR}/usr/local/

#    -DCMAKE_VERBOSE_MAKEFILE=on

make -j
make install

