Summary: Library for moving data (perhaps even at warp speed)
Name: DataMover
Version: 0.1.0
Release: el%(hostnamectl |grep "CPE OS Name" |awk -F":" '{print $6}')
BuildArch: x86_64
License: BSD
URL: https://github.com/majoros/DataMover
Group: Applications/File
Packager: Chris Majoros
Requires: double-conversion >= 3.1.5
Requires: openssl >= 1.1.1
Requires: boost169 = 1.69.0
Requires: boost169-filesystem = 1.69.0
Requires: boost169-system = 1.69.0
BuildRoot: ~/rpmbuild/

%description
DataMover is a fork of facebook's(tm) Warp speed Data Transfer(WDT) library.
DataMover has been refactored to be more flexible. Allowing more endpoints
such as S3. It was build spacificly for the Python module pyDataMover
(https://github.com/majoros/pyDataMover).

%prep
echo PREP

%build
INST_DIR="/var/tmp/dm_install"
mkdir -p $INST_DIR/usr/local

# manually install the following via yum/apt/pacman(with .h files)...
#    - boost 1.6.9
#    - libdouble-conversion 3.1.5
#    - cmake 3.2+
#    - automake
#    - libtool

mkdir -p $RPM_BUILD_ROOT
rm -Rf $RPM_BUILD_ROOT
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
## DataMover
##################################

mkdir -p dm_ins
mkdir -p dm_build
rm -Rf dm_build
mkdir -p dm_build

# FIXME with ${_specdir} ????
# ${_specdir}/../../../ ?????

cp -r /home/cmajoros/git/DataMover ./
ln -s DataMover wdt

cd dm_build
cmake3 \
    ${RPM_BUILD_ROOT}/DataMover \
    -DBUILD_TESTING=off \
    -DFOLLY_SOURCE_DIR=${RPM_BUILD_ROOT}/folly \
    -DBOOST_INCLUDEDIR=/usr/include/boost169 \
    -DBOOST_LIBRARYDIR=/usr/lib64/boost169 \
    -DCMAKE_INSTALL_PREFIX:PATH=${INST_DIR}/usr/local/ \
    -DGFLAGS_LIBRARY=${RPM_BUILD_ROOT}/glog/.libs/libgflags.a \
    -DGFLAGS_LIBRARY=${RPM_BUILD_ROOT}/gflags/build/lib/libgflags.a

make -j
make install

%install
INST_DIR="/var/tmp/dm_install"
cp -Rf ${INST_DIR}/* ${RPM_BUILD_ROOT}/

# TODO: Fix this with cmake commands
find $RPM_BUILD_ROOT/usr/local/  -type f -name "*.so*" -exec chrpath -c -r "\$ORIGIN" {} \;

%clean
INST_DIR="/var/tmp/dm_install"
[ "$RPM_BUILD_ROOT" != "/" ] && rm -rf $RPM_BUILD_ROOT
rm -Rf $INST_DIR

%files
%attr(0755, root, root) /usr/local/bin/*
%attr(0755, root, root) /usr/local/lib64/*
%attr(0755, root, root) /usr/local/include/*



