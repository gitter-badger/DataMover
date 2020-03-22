

# manually install the following via yum/apt/pacman(with .h files)...
#    - boost 1.6.9
#    - libdouble-conversion 3.1.5
#    - cmake 3.2+
#    - automake
#    - libtool

TMP_DIR=/var/tmp/dm_build
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

alias cmake=cmake3

mkdir -p $TMP_DIR
rm -Rf $TMP_DIR
mkdir -p $TMP_DIR
cd $TMP_DIR

##################################
## folly
##################################
git clone https://github.com/facebook/folly.git

##################################
## gflags
##################################

# TODO: this may not be needed as we are not building the cli component.
git clone https://github.com/schuhschuh/gflags.git
mkdir gflags/build
cd gflags/build
cmake3 \
    -DGFLAGS_NAMESPACE=google \
    -DBUILD_SHARED_LIBS=off \
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
    ..
make -j
cd ../../

##################################
## glog
##################################

git clone https://github.com/google/glog.git
cd glog
./autogen.sh
./configure
make -j
cd ../

##################################
## AWS S3
##################################

git clone https://github.com/aws/aws-sdk-cpp.git

##################################
## DataMover
##################################

mkdir -p dm_install
mkdir -p dm_build
rm -Rf dm_install
rm -Rf dm_build
mkdir -p dm_install/usr/local/
mkdir -p dm_build

cd dm_build
cmake3 \
    -DBUILD_TESTING=off \
    -DBUILD_DEPS=ON \
    -DFOLLY_SOURCE_DIR=${TMP_DIR}/folly \
    -DBOOST_INCLUDEDIR=/usr/include/boost169 \
    -DBOOST_LIBRARYDIR=/usr/lib64/boost169 \
    –Daws-sdk-cpp_DIR=${TMP_DIR}/aws-sdk-cpp \
    -DGFLAGS_LIBRARY=${TMP_DIR}/glog/.libs/libgflags.a \
    -DGFLAGS_LIBRARY=${TMP_DIR}/gflags/build/lib/libgflags.a \
    -DCMAKE_INSTALL_PREFIX:PATH=${TMP_DIR}/dm_install/usr/local \
    $SCRIPT_DIR

make -j
make install




