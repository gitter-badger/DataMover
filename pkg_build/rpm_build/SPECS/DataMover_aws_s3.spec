Summary: Amazon's Simple Storage Service(Amazon S3) C++ client librareis
Name: libaws-s3-client
Version: 1.7.301
Release: el%(hostnamectl |grep "CPE OS Name" |awk -F":" '{print $6}')
BuildArch: x86_64
License: Apache-2.0
URL: https://github.com/aws/aws-sdk-cpp
Group: Applications/File
Packager: Chris Majoros
BuildRoot: ~/rpmbuild/

%description
Amazon's Simple Storage Service(Amazon S3) C++ client librareis

%prep
echo PREP

%build

mkdir -p $RPM_BUILD_ROOT
rm -Rf $RPM_BUILD_ROOT
mkdir -p $RPM_BUILD_ROOT
cd $RPM_BUILD_ROOT

#INST_DIR="${RPM_BUILD_ROOT}/dm_aws_install"
INST_DIR="/var/tmp/dm_aws_install"
mkdir -p $INST_DIR/usr/local

##################################
## aws-c-common
##################################
git clone https://github.com/awslabs/aws-c-common
cd aws-c-common
mkdir build
cd build

cmake .. -DBUILD_SHARED_LIBS=on -DCMAKE_INSTALL_PREFIX=${INST_DIR}
make install
cd ../../
##################################
## aws-checksums
##################################
git clone https://github.com/awslabs/aws-checksums
cd aws-checksums
mkdir build
cd build
cmake .. -DBUILD_SHARED_LIBS=on -DCMAKE_INSTALL_PREFIX=${INST_DIR}
make install
cd ../../

##################################
## aws-c-event-stream
##################################
git clone https://github.com/awslabs/aws-c-event-stream
cd aws-c-event-stream
mkdir build
cd build

cmake .. -DBUILD_SHARED_LIBS=on -DCMAKE_INSTALL_PREFIX=${INST_DIR} -DCMAKE_PREFIX_PATH=${INST_DIR}
make install
cd ../../

##################################
## aws-sdk-cpp
##################################
git clone https://github.com/aws/aws-sdk-cpp.git
cd aws-sdk-cpp
mkdir build
cd build

cmake \
    -DBUILD_ONLY="s3" \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX=${INST_DIR} \
    -DCMAKE_PREFIX_PATH=${INST_DIR} \
    -DCMAKE_INSTALL_RPATH="\$ORIGIN" \
    -DCMAKE_INSTALL_RPATH_USE_LINK_PATH=false \
    -DCMAKE_EXE_LINKER_FLAGS="-Wl,--enable-new-dtags" \
    ..
make install

%install
INST_DIR="/var/tmp/dm_aws_install"
mkdir -p $RPM_BUILD_ROOT/usr/local/
cp -Rf ${INST_DIR}/lib64 $RPM_BUILD_ROOT/usr/local/
cp -Rf ${INST_DIR}/include $RPM_BUILD_ROOT/usr/local/

# TODO: Fix this with cmake commands
find $RPM_BUILD_ROOT/usr/local/  -type f -name "*.so*" -exec chrpath -c -r "\$ORIGIN" {} \;

%clean
INST_DIR="/var/tmp/dm_aws_install"
[ "$RPM_BUILD_ROOT" != "/" ] && rm -rf $RPM_BUILD_ROOT
rm -Rf $INST_DIR

%files
%attr(0755, root, root) /usr/local/lib64/*
%attr(0755, root, root) /usr/local/include/*



