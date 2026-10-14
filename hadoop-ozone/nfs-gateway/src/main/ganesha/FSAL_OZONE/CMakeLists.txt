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

if(NOT OZONE_NFS_GATEWAY_SOURCE)
  message(FATAL_ERROR "OZONE_NFS_GATEWAY_SOURCE is required to build FSAL_OZONE")
endif()

set(OZONE_NFS_GRPC ON CACHE BOOL "Build the authenticated Ozone NFS adapter" FORCE)
add_subdirectory("${OZONE_NFS_GATEWAY_SOURCE}"
  "${CMAKE_CURRENT_BINARY_DIR}/ozone-nfs-gateway" EXCLUDE_FROM_ALL)

add_library(fsalozone MODULE ozone_main.c ozone_export.c ozone_handle.c)
add_sanitizers(fsalozone)
target_link_libraries(fsalozone
  ganesha_nfsd
  ozone_nfs_grpc
  ozone_nfs_handle
  ${SYSTEM_LIBRARIES}
  ${LTTNG_LIBRARIES}
  ${LDFLAG_DISALLOW_UNDEF}
)
set_target_properties(fsalozone PROPERTIES LINKER_LANGUAGE CXX VERSION 1.0.0 SOVERSION 1)
install(TARGETS fsalozone COMPONENT fsal DESTINATION ${FSAL_DESTINATION})
