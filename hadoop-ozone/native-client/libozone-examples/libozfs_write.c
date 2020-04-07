#include "ozfs.h"

#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>

int main(int argc, char **argv) {
    ozfsFS fs;
    const char *writeFileName = argv[1];
    off_t fileTotalSize = strtoul(argv[2], NULL, 10);
    long long tmpBufferSize = strtoul(argv[3], NULL, 10);
    tSize bufferSize;
    ozfsFile writeFile;
    char* buffer;
    int i;
    off_t nrRemaining;
    tSize curSize;
    tSize written;
    if (argc != 4) {
        fprintf(stderr, "Usage: hdfs_write <filename> <filesize> <buffersize>\n");
        exit(-1);
    }
    fs = ozfsConnect("127.0.0.1", 9862,"bucket4","vol4");
    if (!fs) {
        fprintf(stderr, "Oops! Failed to connect to ozfs!\n");
        exit(-1);
    }
    if(fileTotalSize == ULONG_MAX && errno == ERANGE) {
      fprintf(stderr, "invalid file size %s - must be <= %lu\n", argv[2], ULONG_MAX);
      exit(-3);
    }
    if(tmpBufferSize > INT_MAX) {
      fprintf(stderr, "invalid buffer size libhdfs API write chunks must be <= %d\n",INT_MAX);
      exit(-3);
    }
    bufferSize = (tSize)tmpBufferSize;
    writeFile = ozfsOpenFile(fs, writeFileName, O_WRONLY, bufferSize, 0, 0);
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
    for (nrRemaining = fileTotalSize; nrRemaining > 0; nrRemaining -= bufferSize ) {
      curSize = ( bufferSize < nrRemaining ) ? bufferSize : (tSize)nrRemaining;
      if ((written = ozfsWrite(fs, writeFile, (void*)buffer, curSize)) != curSize) {
        fprintf(stderr, "ERROR: ozfsWrite returned an error on write: %d\n", written);
        exit(-3);
      }
    }
    free(buffer);
    ozfsCloseFile(fs, writeFile);
    ozfsDisconnect(fs);
    return 0;
}

