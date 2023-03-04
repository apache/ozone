#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#
# CMake configuration.
#

cmake_minimum_required(VERSION 2.8)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fPIC")
project(ozone_native)
set(CMAKE_BUILD_TYPE Release)
find_package(JNI REQUIRED)
include_directories(${JNI_INCLUDE_DIRS})
set(CMAKE_CXX_STANDARD ${CMAKE_STANDARDS})

set(linked_libraries "")
if(NOT GENERATED_JAVAH)
    message(FATAL_ERROR "You must set the CMake variable GENERATED_JAVAH")
endif()
include_directories(${GENERATED_JAVAH})
if(${SST_DUMP_INCLUDE})
    include_directories(${ROCKSDB_HEADERS})
    set(SOURCE_FILES ${NATIVE_DIR}/SSTDumpTool.cpp ${NATIVE_DIR}/PipeInputStream.cpp ${NATIVE_DIR}/Pipe.h ${NATIVE_DIR}/Pipe.cpp ${NATIVE_DIR}/cplusplus_to_java_convert.h)
    ADD_LIBRARY(rocksdb STATIC IMPORTED)
    set_target_properties(
            rocksdb
            PROPERTIES
            IMPORTED_LOCATION ${ROCKSDB_LIB}/librocksdb_debug.a)
    ADD_LIBRARY(rocks_tools STATIC IMPORTED)
    set_target_properties(
            rocks_tools
            PROPERTIES
            IMPORTED_LOCATION ${ROCKSDB_LIB}/librocksdb_tools_debug.a)
    ADD_LIBRARY(bz2 STATIC IMPORTED)
    set_target_properties(
            bz2
            PROPERTIES
            IMPORTED_LOCATION ${BZIP2_LIB}/libbz2.a)
    ADD_LIBRARY(zlib STATIC IMPORTED)
    set_target_properties(
            zlib
            PROPERTIES
            IMPORTED_LOCATION ${ZLIB_LIB}/libz.a)
    ADD_LIBRARY(lz4 STATIC IMPORTED)
    set_target_properties(
            lz4
            PROPERTIES
            IMPORTED_LOCATION ${LZ4_LIB}/liblz4.a)
    ADD_LIBRARY(snappy STATIC IMPORTED)
    set_target_properties(
            snappy
            PROPERTIES
            IMPORTED_LOCATION ${SNAPPY_LIB}/libsnappy.a)
    ADD_LIBRARY(zstd STATIC IMPORTED)
        set_target_properties(
                zstd
                PROPERTIES
                IMPORTED_LOCATION ${ZSTD_LIB}/libzstd.a)
    set(linked_libraries ${linked_libraries} bz2 zlib rocks_tools rocksdb lz4 snappy zstd)
endif()
add_library(ozone_rocksdb_tools SHARED ${SOURCE_FILES})
target_link_libraries(ozone_rocksdb_tools ${linked_libraries})
