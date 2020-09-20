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
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include<string.h>

int main(int argc, char **argv) {
    o3fsFS fs;
    const char *writeFileName = argv[1];
    off_t fileTotalSize = strtoul(argv[2], NULL, 10);
    long long tmpBufferSize = strtoul(argv[3], NULL, 10);
    const char *host = argv[4];
    tPort port = atoi(argv[5]);
    const char *bucket = argv[6];
    const char *volume = argv[7];
    tSize bufferSize;
    o3fsFile writeFile;
    char* buffer;
    int i;
    off_t nrRemaining;
    tSize curSize;
    tSize written;
    char message[110] = "Usage: o3fs_write <filename> <filesize> <buffersize>";
    strcat(message, " <host-name> <port> <bucket-name> <volume-name>\n");
    if (argc != 8) {
        fprintf(stderr, message);
        exit(-1);
    }
    fs = o3fsConnect(host, port, bucket, volume);
    if (!fs) {
        fprintf(stderr, "Oops! Failed to connect to o3fs!\n");
        exit(-1);
    }
    if(fileTotalSize == ULONG_MAX && errno == ERANGE) {
      fprintf(stderr, "invalid file size %s - must be <= %lu\n",
       argv[2], ULONG_MAX);
      exit(-3);
    }
    if(tmpBufferSize > INT_MAX) {
      fprintf(stderr,
       "invalid buffer size libhdfs API write chunks must be <= %d\n", INT_MAX);
      exit(-3);
    }
    bufferSize = (tSize)tmpBufferSize;
    writeFile = o3fsOpenFile(fs, writeFileName, O_WRONLY, bufferSize, 0, 0);
    if (!writeFile) {
        fprintf(stderr, "Failed to open %s for writing!\n", writeFileName);
        exit(-2);
    }
    buffer = malloc(sizeof(char) * bufferSize);
    if(buffer == NULL) {
        fprintf(stderr, "Could not allocate buffer of size %d\n", bufferSize);
        return -2;
    }
    for (i=0; i < bufferSize; ++i) {
        buffer[i] = 'a' + (i%26);
    }
    for (nrRemaining = fileTotalSize; nrRemaining > 0;
     nrRemaining -= bufferSize ) {
      curSize = ( bufferSize < nrRemaining ) ? bufferSize : (tSize)nrRemaining;
      if ((written = o3fsWrite(fs, writeFile, (void*)buffer,
       curSize)) != curSize) {
        fprintf(stderr, "ERROR: o3fsWrite returned an error on write: %d\n",
         written);
        exit(-3);
      }
    }
    free(buffer);
    o3fsCloseFile(fs, writeFile);
    o3fsDisconnect(fs);
    return 0;
}