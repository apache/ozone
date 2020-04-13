/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef OZFS_DOT_H    
#define OZFS_DOT_H

#include "hdfs/hdfs.h"

struct hdfs_internal;
typedef struct hdfs_internal* ozfsFS;

struct hdfsFile_internal;
typedef struct hdfsFile_internal* ozfsFile;

ozfsFS ozfsConnect(const char* nn, tPort port, const char* bucket,
 const char* volume);

ozfsFile ozfsOpenFile(ozfsFS fs, const char *path, int flags, int bufferSize,
 short replication, tSize blockSize);

tSize ozfsRead(ozfsFS fs, ozfsFile f, void* buffer, tSize length);

int ozfsCloseFile(ozfsFS fs, ozfsFile file);

int ozfsDisconnect(ozfsFS fs);

tSize ozfsWrite(ozfsFS fs, ozfsFile f, const void* buffer, tSize length);

#endif

