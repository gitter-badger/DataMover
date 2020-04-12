Summary: Library for moving data (perhaps even at warp speed)
Name: DataMover
Version: 0.1.0
Release: el%(hostnamectl |grep "CPE OS Name" |awk -F":" '{print $6}')
BuildArch: x86_64
License: BSD
URL: https://github.com/majoros/DataMover
Group: Applications/File
Packager: Chris Majoros
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
git clone https://github.com/schuhschuh/gflags.git
mkdir gflags/build
cd gflags/build
cmake \
    -DGFLAGS_NAMESPACE=google \
    -DBUILD_SHARED_LIBS=on \
    -DCMAKE_INSTALL_PREFIX:PATH=${INST_DIR}/usr/local/ \
    ..
make
make install
cd $RPM_BUILD_ROOT

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
cd $RPM_BUILD_ROOT

##################################
## Double conversion
##################################
git clone https://github.com/floitsch/double-conversion.git
cd double-conversion;
mkdir build
cd build
cmake \
    .. \
    -DCMAKE_PREFIX_PATH="${INST_DIR}/usr/local/" \
    -DCMAKE_INSTALL_PREFIX="${INST_DIR}/usr/local/" \
    -DBUILD_SHARED_LIBS=on

make -j
make install
cd $RPM_BUILD_ROOT

##################################
## DataMover
##################################

mkdir -p dm_build
rm -Rf dm_build
mkdir -p dm_build

cp -r ${HOME}/git/DataMover ./
ln -s DataMover wdt

cd ${RPM_BUILD_ROOT}/dm_build

cmake3 \
    ${RPM_BUILD_ROOT}/DataMover \
    -DBUILD_TESTING=off \
    -DFOLLY_SOURCE_DIR=${RPM_BUILD_ROOT}/folly \
    -DBOOST_INCLUDEDIR=/usr/include/boost169 \
    -DBOOST_LIBRARYDIR=/usr/lib64/boost169 \
    -DCMAKE_INSTALL_PREFIX:PATH=${INST_DIR}/usr/local/

#    -DCMAKE_VERBOSE_MAKEFILE=on

make -j
make install

%install
INST_DIR="/var/tmp/dm_install"
mkdir -p ${RPM_BUILD_ROOT}/usr/local/DataMover

cp -Rf ${INST_DIR}/usr/local/include ${RPM_BUILD_ROOT}/usr/local/DataMover/
cp -Rf ${INST_DIR}/usr/local/lib64 ${RPM_BUILD_ROOT}/usr/local/DataMover/
if [ -d "${INST_DIR}/usr/local/lib" ]
then
    cp -Rf ${INST_DIR}/usr/local/lib/* ${RPM_BUILD_ROOT}/usr/local/DataMover/lib64/
fi
cp -Rf ${INST_DIR}/usr/local/bin ${RPM_BUILD_ROOT}/usr/local/DataMover/

# TODO: Fix this with cmake commands
find $RPM_BUILD_ROOT/usr/local/  -type f -name "*.so*" -exec chrpath -c -r "\$ORIGIN" {} \;

%clean
INST_DIR="/var/tmp/dm_install"
[ "$RPM_BUILD_ROOT" != "/" ] && rm -rf $RPM_BUILD_ROOT
#rm -Rf $INST_DIR

%files
%attr(0755, root, root) /usr/local/DataMover/bin/*
%attr(0755, root, root) /usr/local/DataMover/lib64/*
%attr(0755, root, root) /usr/local/DataMover/include/*



