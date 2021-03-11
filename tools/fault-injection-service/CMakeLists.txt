#
# Licensed to the Apache Software Foundation (ASF) under one or more           
# contributor license agreements.  See the NOTICE file distributed with this   
# work for additional information regarding copyright ownership.  The ASF      
# licenses this file to you under the Apache License, Version 2.0 (the         
# "License"); you may not use this file except in compliance with the License. 
# You may obtain a copy of the License at                                      
# <p>                                                                          
# http://www.apache.org/licenses/LICENSE-2.0                                   
# <p>                                                                          
# Unless required by applicable law or agreed to in writing, software          
# distributed under the License is distributed on an "AS IS" BASIS,WITHOUT     
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the     
# License for the specific language governing permissions and limitations under
# the License.                                                                 
#
# cmake build file for C++ failure_injection_service.
# Assumes protobuf and gRPC have been installed using cmake.

cmake_minimum_required(VERSION 2.8)

project(FailureInjectionService C CXX)

set(BASE_DIR ".")
set(FS_DIR "${BASE_DIR}/FileSystem")
set(SRV_DIR "${BASE_DIR}/Service/cpp")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -fpermissive -Wall")

# This assumes that gRPC and all its dependencies are already installed
# on this system, so they can be located by find_package().

# Find Protobuf installation
# Looks for protobuf-config.cmake file installed by Protobuf's cmake
# installation.
set(protobuf_MODULE_COMPATIBLE TRUE)
find_package(Protobuf CONFIG REQUIRED)
message(STATUS "Using protobuf ${protobuf_VERSION}")

set(_PROTOBUF_LIBPROTOBUF protobuf::libprotobuf)
set(_PROTOBUF_PROTOC $<TARGET_FILE:protobuf::protoc>)

# Find gRPC installation
# Looks for gRPCConfig.cmake file installed by gRPC's cmake installation.
find_package(gRPC CONFIG REQUIRED)
message(STATUS "Using gRPC ${gRPC_VERSION}")

set(_GRPC_GRPCPP_UNSECURE gRPC::grpc++_unsecure)
set(_GRPC_CPP_PLUGIN_EXECUTABLE $<TARGET_FILE:gRPC::grpc_cpp_plugin>)

# Proto file
get_filename_component(hw_proto
    "${BASE_DIR}/Service/protos/failure_injection_service.proto"
    ABSOLUTE)
get_filename_component(hw_proto_path "${hw_proto}" PATH)

# Generated sources
set(hw_proto_srcs
    "${CMAKE_CURRENT_BINARY_DIR}/failure_injection_service.pb.cc")
set(hw_proto_hdrs
    "${CMAKE_CURRENT_BINARY_DIR}/failure_injection_service.pb.h")
set(hw_grpc_srcs
    "${CMAKE_CURRENT_BINARY_DIR}/failure_injection_service.grpc.pb.cc")
set(hw_grpc_hdrs
    "${CMAKE_CURRENT_BINARY_DIR}/failure_injection_service.grpc.pb.h")
add_custom_command(
      OUTPUT "${hw_proto_srcs}" "${hw_proto_hdrs}"
             "${hw_grpc_srcs}" "${hw_grpc_hdrs}"
      COMMAND ${_PROTOBUF_PROTOC}
      ARGS --grpc_out "${CMAKE_CURRENT_BINARY_DIR}"
        --cpp_out "${CMAKE_CURRENT_BINARY_DIR}"
        -I "${hw_proto_path}"
        --plugin=protoc-gen-grpc="${_GRPC_CPP_PLUGIN_EXECUTABLE}"
        "${hw_proto}"
      DEPENDS "${hw_proto}")

# Include generated *.pb.h files
include_directories("${CMAKE_CURRENT_BINARY_DIR}"
    "${FS_DIR}"
    "${SRV_DIR}")

#add_compile_options("-fpermissive")

# Build server
add_executable(failure_injector_svc_server 
    ${FS_DIR}/failure_injector_fs.cc ${FS_DIR}/failure_injector.cc
    ${SRV_DIR}/failure_injector_svc_server.cc ${SRV_DIR}/run_grpc_service.cc 
    ${hw_proto_srcs}
    ${hw_grpc_srcs})
target_link_libraries(failure_injector_svc_server
    ${_GRPC_GRPCPP_UNSECURE}
    fuse3 
    ${_PROTOBUF_LIBPROTOBUF})

# Build client
add_executable(failure_injector_svc_client 
    "${SRV_DIR}/failure_injector_svc_client.cc" 
    ${hw_proto_srcs} 
    ${hw_grpc_srcs})
target_link_libraries(failure_injector_svc_client 
    ${_GRPC_GRPCPP_UNSECURE}
    ${_PROTOBUF_LIBPROTOBUF})

# Build unit tests
set(CPP_UNIT_FUSE cpp_unit)
foreach(_target
  TestFilePathFailures
  TestFailureInjector)
    add_executable(${_target}
                   "${FS_DIR}/${CPP_UNIT_FUSE}/${_target}.cc"
                   ${FS_DIR}/failure_injector.cc)
    target_link_libraries(${_target} 
            cppunit)
endforeach()
