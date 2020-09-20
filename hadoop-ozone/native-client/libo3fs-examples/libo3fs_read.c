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
#include <stdio.h>
#include <stdlib.h>
#include<string.h>

int main(int argc, char **argv) {
    o3fsFS fs;
    const char *rfile = argv[1];
    tSize bufferSize = strtoul(argv[3], NULL, 10);
    const char *host = argv[4];
    tPort port = atoi(argv[5]);
    const char *bucket = argv[6];
    const char *volume = argv[7];
    o3fsFile readFile;
    char* buffer;
    tSize curSize;
    char message[110] = "Usage: o3fs_read <filename> <filesize> <buffersize>";
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
    readFile = o3fsOpenFile(fs, rfile, O_RDONLY, bufferSize, 0, 0);
    if (!readFile) {
        fprintf(stderr, "Failed to open %s for writing!\n", rfile);
        exit(-2);
    }
    // data to be written to the file
    buffer = malloc(sizeof(char) * bufferSize);
    if(buffer == NULL) {
        return -2;
    }
    // read from the file
    curSize = bufferSize;
    for (; curSize == bufferSize;) {
        curSize = o3fsRead(fs, readFile, (void*)buffer, curSize);
    }
    free(buffer);
    o3fsCloseFile(fs, readFile);
    o3fsDisconnect(fs);
    return 0;
}