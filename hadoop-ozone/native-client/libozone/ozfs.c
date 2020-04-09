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

#include "ozfs.h"
#include "hdfs/hdfs.h"
#include <fcntl.h>
#include <inttypes.h>
#include <stdio.h>
#include <string.h>


ozfsFS ozfsConnect(const char *host, tPort port, const char *bucket, const char *vol)
{
    struct hdfsBuilder *bld = hdfsNewBuilder();
    int len = 0;
    if (!bld)
        return NULL;
    len = strlen(host) + strlen(bucket) + strlen(vol) + strlen("o3fs://");
    char string[len + 2];
    snprintf(string, len + 3, "o3fs://%s.%s.%s", bucket, vol, host);
    printf("URI : %s\n", string);
    hdfsBuilderSetNameNode(bld, string);
    hdfsBuilderSetNameNodePort(bld, port);
    return (ozfsFS)hdfsBuilderConnect(bld);
}
ozfsFile ozfsOpenFile(ozfsFS fs, const char *path, int flags, int bufferSize, short replication, tSize blockSize){
    return (ozfsFile)hdfsOpenFile((hdfsFS)fs, path, flags, bufferSize, replication, blockSize);
}

tSize ozfsRead(ozfsFS fs, ozfsFile f, void* buffer, tSize length){
    return hdfsRead((hdfsFS)fs, (hdfsFile)f, buffer, length);
}

int ozfsCloseFile(ozfsFS fs, ozfsFile file){
    return hdfsCloseFile((hdfsFS)fs, (hdfsFile)file);
}

int ozfsDisconnect(ozfsFS fs){
    return hdfsDisconnect((hdfsFS)fs);
}

tSize ozfsWrite(ozfsFS fs, ozfsFile f, const void* buffer, tSize length){
    return hdfsWrite((hdfsFS)fs, (hdfsFile)f, buffer, length);
}

