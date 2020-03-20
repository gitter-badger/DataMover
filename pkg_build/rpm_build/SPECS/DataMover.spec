Summary: Library for moving data (perhaps even at warp speed)
Name: DataMover
Version: 0.1.0
Release: 1
License: BSD
URL: https://github.com/majoros/DataMover
Group: Applications/File
Packager: Chris Majoros
Requires: double-conversion
Requires: openssl
Requires: boost169
BuildRoot: ~/rpmbuild/

%description
DataMover is a fork of facebook's(tm) Warp speed Data Transfer(WDT) library.
DataMover has been refactored to be more flexible. Allowing more endpoints
such as S3. It was build spacificly for the Python module pyDataMover
(https://github.com/majoros/pyDataMover).

%prep
echo PREP

%build

echo "BUILDROOT = $RPM_BUILD_ROOT"

# manually install the following via yum/apt/pacman(with .h files)...
#    - boost 1.6.9
#    - libdouble-conversion 3.1.5
#    - cmake 3.2+
#    - automake
#    - libtool

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

[ "$RPM_BUILD_ROOT" != "/" ] && rm -rf $RPM_BUILD_ROOT
mkdir -p $RPM_BUILD_ROOT
cd $RPM_BUILD_ROOT

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
cmake \
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
./configure #--without-gflags
make -j
cd ../

##################################
## DataMover
##################################

mkdir -p dm_install
rm -Rf dm_install
mkdir -p dm_install/usr/local

cmake \
    $SCRIPT_DIR \
    -DBUILD_TESTING=off \
    -DFOLLY_SOURCE_DIR=${RPM_BUILD_ROOT}/folly \
    -DBOOST_INCLUDEDIR=/usr/include/boost169 \
    -DBOOST_LIBRARYDIR=/usr/lib64/boost169 \
    -DGFLAGS_LIBRARY=${RPM_BUILD_ROOT}/glog/.libs/libgflags.a \
    -DGFLAGS_LIBRARY=${RPM_BUILD_ROOT}/gflags/build/lib/libgflags.a \
    -DCMAKE_INSTALL_PREFIX:PATH=${RPM_BUILD_ROOT}/dm_install/usr/local/

make -j
make install

%install
cp -Rf ${RPM_BUILD_ROOT}/dm_install/* $RPM_BUILD_ROOT

[ "$RPM_BUILD_ROOT" != "/" ] && rm -rf $RPM_BUILD_ROOT

%files
%attr(0744, root, root) /usr/local/bin/*
%attr(0644, root, root) /usr/local/lib/*
%attr(0644, root, root) /usr/local/include/*



