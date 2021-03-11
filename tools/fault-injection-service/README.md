Fault Injection Service
========================

About
------
Fault Injection Service is designed to validate Ozone under heavy stress and
failed or failing system components. This service runs independently of Ozone
and offers a client tool to inject errors.

Development Status                                                              
------------------
Currently this service can inject errors on the IO path. The next step would be
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

Installation                                                                    
------------ 
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
    - Simulate temporary or on-disk data corruption on IO path.
    - Resetting specific or all the failures injected so far.

- some unit test binaries

Security implications                                                           
--------------------- 
TBD
