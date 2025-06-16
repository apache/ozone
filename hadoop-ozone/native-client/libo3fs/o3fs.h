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

#ifndef O3FS_DOT_H
#define O3FS_DOT_H

#include "hdfs.h"

struct hdfs_internal;
typedef struct hdfs_internal* o3fsFS;

struct hdfsFile_internal;
typedef struct hdfsFile_internal* o3fsFile;

o3fsFS o3fsConnect(const char* nn, tPort port, const char* bucket,
 const char* volume);

o3fsFile o3fsOpenFile(o3fsFS fs, const char *path, int flags, int bufferSize,
 short replication, tSize blockSize);

tSize o3fsRead(o3fsFS fs, o3fsFile f, void* buffer, tSize length);

int o3fsCloseFile(o3fsFS fs, o3fsFile file);

int o3fsDisconnect(o3fsFS fs);

tSize o3fsWrite(o3fsFS fs, o3fsFile f, const void* buffer, tSize length);

#endif
