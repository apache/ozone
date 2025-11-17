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

cmake_minimum_required(VERSION 2.8...3.31)
add_definitions(-D_GLIBCXX_USE_CXX11_ABI=0)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fPIC")
project(ozone_native)
set(CMAKE_BUILD_TYPE Release)
find_package(JNI REQUIRED)
include_directories(${JNI_INCLUDE_DIRS})
set(CMAKE_CXX_STANDARD ${CMAKE_STANDARDS})

set(CMAKE_SKIP_BUILD_RPATH FALSE)

set(CMAKE_BUILD_WITH_INSTALL_RPATH FALSE)

set(CMAKE_INSTALL_RPATH "")

set(CMAKE_INSTALL_RPATH_USE_LINK_PATH FALSE)

if(NOT GENERATED_JAVAH)
    message(FATAL_ERROR "You must set the CMake variable GENERATED_JAVAH")
endif()
include_directories(${GENERATED_JAVAH})
if(${SST_DUMP_INCLUDE})
    include_directories(${ROCKSDB_HEADERS})
    set(SOURCE_FILES ${NATIVE_DIR}/ManagedRawSSTFileReader.cpp ${NATIVE_DIR}/ManagedRawSSTFileIterator.cpp ${NATIVE_DIR}/cplusplus_to_java_convert.h)
    ADD_LIBRARY(rocks_tools STATIC IMPORTED)
    set_target_properties(
            rocks_tools
            PROPERTIES
            IMPORTED_LOCATION ${ROCKSDB_TOOLS_LIB}/librocksdb_tools.a)
endif()

add_library(ozone_rocksdb_tools SHARED ${SOURCE_FILES})


target_link_libraries(ozone_rocksdb_tools PRIVATE ${ROCKSDB_LIB})
target_link_libraries(ozone_rocksdb_tools PRIVATE rocks_tools)
set_target_properties(ozone_rocksdb_tools PROPERTIES
        BUILD_WITH_INSTALL_RPATH FALSE
        LINK_FLAGS "-Wl,-rpath -Wl,'$ORIGIN'")
