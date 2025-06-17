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

#include "o3fs.h"
#include "hdfs.h"
#include <fcntl.h>
#include <inttypes.h>
#include <stdio.h>
#include <string.h>
#define O3FS "o3fs://"

o3fsFS o3fsConnect(const char *host, tPort port,
 const char *bucket, const char *vol)
{
    struct hdfsBuilder *bld = hdfsNewBuilder();
    int len = 0;
    if (!bld){
        return NULL;
    }
    len = strlen(host) + strlen(bucket) + strlen(vol) + strlen(O3FS);
    char string[len + 2];
    snprintf(string, len + 3, "%s%s.%s.%s", O3FS, bucket, vol, host);
    // After snprintf command,
    // string = o3fs://bucket4.vol4.127.0.0.1 (URI without port)
    //port will be added to URI in hdfsBuilerConnect() function below.
    //finally URI: o3fs://bucket4.vol4.127.0.0.1:9862
    hdfsBuilderSetNameNode(bld, string);
    hdfsBuilderSetNameNodePort(bld, port);
    return (o3fsFS)hdfsBuilderConnect(bld);
}
o3fsFile o3fsOpenFile(o3fsFS fs, const char *path, int flags, int bufferSize,
 short replication, tSize blockSize){
    return (o3fsFile)hdfsOpenFile((hdfsFS)fs, path, flags, bufferSize,
     replication, blockSize);
}

tSize o3fsRead(o3fsFS fs, o3fsFile f, void* buffer, tSize length){
    return hdfsRead((hdfsFS)fs, (hdfsFile)f, buffer, length);
}

int o3fsCloseFile(o3fsFS fs, o3fsFile file){
    return hdfsCloseFile((hdfsFS)fs, (hdfsFile)file);
}

int o3fsDisconnect(o3fsFS fs){
    return hdfsDisconnect((hdfsFS)fs);
}

tSize o3fsWrite(o3fsFS fs, o3fsFile f, const void* buffer, tSize length){
    return hdfsWrite((hdfsFS)fs, (hdfsFile)f, buffer, length);
}
