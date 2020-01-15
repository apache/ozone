NoiseInjector
==============

About
------
TBD

Development Status                                                              
------------------
TBD

Supported Platforms                                                             
------------------- 
Linux

Dependencies
-------------
libfuse
libgrpc
cmake

Installation                                                                    
------------ 
Goto Build subdirectory
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
    - Reseting specific or all the failures injected so far.

- some unit test binaries

Security implications                                                           
--------------------- 
TBD
