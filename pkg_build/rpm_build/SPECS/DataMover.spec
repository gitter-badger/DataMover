Summary: Library for moving data (perhaps even at warp speed)
Name: DataMover
Version: 0.1.0
Release: el%(hostnamectl |grep "CPE OS Name" |awk -F":" '{print $6}')
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

%install
cp -Rf /var/tmp/dm_build/dm_install/* $RPM_BUILD_ROOT

%clean
[ "$RPM_BUILD_ROOT" != "/" ] && rm -rf $RPM_BUILD_ROOT

%files
%attr(0744, root, root) /usr/local/bin/*
%attr(0644, root, root) /usr/local/lib/*
%attr(0644, root, root) /usr/local/include/*



