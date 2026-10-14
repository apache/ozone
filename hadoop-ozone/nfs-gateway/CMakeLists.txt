# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

cmake_minimum_required(VERSION 3.16)
project(ozone_nfs_native C)

add_library(ozone_nfs_handle STATIC src/main/native/ozone_handle.c)
target_include_directories(ozone_nfs_handle PUBLIC src/main/native)
set_target_properties(ozone_nfs_handle PROPERTIES C_STANDARD 99 C_STANDARD_REQUIRED YES POSITION_INDEPENDENT_CODE ON)
if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang")
  target_compile_options(ozone_nfs_handle PRIVATE -Wall -Wextra -Werror)
endif()

include(CTest)
if(BUILD_TESTING)
  add_executable(test_ozone_handle src/test/native/test_ozone_handle.c)
  target_link_libraries(test_ozone_handle PRIVATE ozone_nfs_handle)
  set_target_properties(test_ozone_handle PROPERTIES C_STANDARD 99 C_STANDARD_REQUIRED YES)
  if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(test_ozone_handle PRIVATE -Wall -Wextra -Werror)
  endif()
  add_test(NAME ozone_nfs_handle_contract COMMAND test_ozone_handle
      "${CMAKE_CURRENT_SOURCE_DIR}/../common/src/test/resources/nfs/file-handles.txt")
endif()

option(OZONE_NFS_GRPC "Build the authenticated C++ gRPC adapter" OFF)
if(OZONE_NFS_GRPC)
  enable_language(CXX)
  find_package(Protobuf REQUIRED)
  find_package(gRPC CONFIG REQUIRED)
  set(nfs_proto_dir "${CMAKE_CURRENT_SOURCE_DIR}/../interface-client/src/main/proto")
  set(nfs_generated_dir "${CMAKE_CURRENT_BINARY_DIR}/generated")
  file(MAKE_DIRECTORY "${nfs_generated_dir}")
  add_custom_command(
    OUTPUT "${nfs_generated_dir}/NfsGatewayProtocol.pb.cc" "${nfs_generated_dir}/NfsGatewayProtocol.pb.h"
           "${nfs_generated_dir}/NfsGatewayProtocol.grpc.pb.cc" "${nfs_generated_dir}/NfsGatewayProtocol.grpc.pb.h"
    COMMAND protobuf::protoc
      "--proto_path=${nfs_proto_dir}"
      "--cpp_out=${nfs_generated_dir}"
      "--grpc_out=${nfs_generated_dir}"
      --plugin=protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>
      "${nfs_proto_dir}/NfsGatewayProtocol.proto"
    DEPENDS "${nfs_proto_dir}/NfsGatewayProtocol.proto"
    VERBATIM)
  add_library(ozone_nfs_grpc STATIC src/main/native/ozone_grpc.cc
      "${nfs_generated_dir}/NfsGatewayProtocol.pb.cc" "${nfs_generated_dir}/NfsGatewayProtocol.grpc.pb.cc")
  target_include_directories(ozone_nfs_grpc PUBLIC src/main/native PRIVATE "${nfs_generated_dir}")
  target_link_libraries(ozone_nfs_grpc PRIVATE gRPC::grpc++ protobuf::libprotobuf)
  set_target_properties(ozone_nfs_grpc PROPERTIES CXX_STANDARD 17 CXX_STANDARD_REQUIRED YES POSITION_INDEPENDENT_CODE ON)
  if(BUILD_TESTING)
    add_executable(test_ozone_grpc_tls src/test/native/test_ozone_grpc_tls.cc)
    target_link_libraries(test_ozone_grpc_tls PRIVATE ozone_nfs_grpc ozone_nfs_handle)
    set_target_properties(test_ozone_grpc_tls PROPERTIES CXX_STANDARD 17 CXX_STANDARD_REQUIRED YES)
  endif()
  if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(ozone_nfs_grpc PRIVATE -Wall -Wextra -Werror)
  endif()
endif()
