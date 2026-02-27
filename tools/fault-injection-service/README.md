Fault Injection Service
========================

About
------
Fault Injection Service is designed to validate Ozone under heavy stress and
failed or failing system components. This service runs independently of Ozone
and offers a client tool to inject errors.

Development Status                                                              
------------------
Currently this service can inject errors on the I/O path. The next step would be
to add ability to inject similar errors on the network path as well.

Supported Platforms                                                             
------------------- 
Linux

Dependencies
-------------
- libfuse3
	yum install fuse3.x86_64 fuse3-devel.x86_64 fuse3-libs.x86_64
- protobuf 2.5 or higher
- grpc
	built/installed from sources
- cmake 3.14
	built/installed from sources
- cppunit & cppunit-devel

Building Dependencies
======================

Building libfuse3 from the sources
------------------------------------
    - You can get it from https://github.com/libfuse/libfuse/releases/
        * https://github.com/libfuse/libfuse/releases/tag/fuse-3.10.2
    - follow the README.md for building libfuse
    - you may need to get "meson-0.42.0" or above to build this.

CMAKE
======
    - this will need cmake-3.14.0 or higher
    - if required build from the sources. (tested with cmake-3.14.0 and
       cmake-3.6.2)
    
Building grpc from the sources
------------------------------------
    - https://grpc.io/docs/languages/cpp/quickstart/
    - sudo apt install -y build-essential autoconf libtool pkg-config
    - git clone --recurse-submodules -b v1.35.0 https://github.com/grpc/grpc
    - Follow build instructions
        - mkdir -p cmake/build
        - pushd cmake/build
        - cmake -DgRPC_INSTALL=ON -DgRPC_BUILD_TESTS=OFF\
                 -DCMAKE_INSTALL_PREFIX=$MY_INSTALL_DIR  ../..
        - make -j
        - make install 

Finally
-------
    - Make sure all the dependencies are in $PATH.
    - This build was last tested on (debian 4.18.5-1.el7.elrepo.x86_64
       GNU/Linux) and Ubuntu 18:0.1.0.0-4.

Installation                                                                    
=============
mkdir Build 
cd Build
do 'cmake ..'
do 'make'


This will build following binaries:
- failure_injector_svc_server
    usage : failure_injector_svc_server /mnt
    It will create a Fuse mountpoint on /mnt and also start a grpc server
    to listen for failure injections on this mountpoint.

- failure_injector_svc_client
    This is a grpc client that can be used to inject failures on the /mnt
    mountpoint above. Currently it supports,
    - Injecting delays on various filesystem interfaces.
    - Injecting a specific failure on a specific path for a specific operation.
    - Simulate temporary or on-disk data corruption on I/O path.
    - Resetting specific or all the failures injected so far.

- some unit test binaries

Security implications                                                           
--------------------- 
TBD
