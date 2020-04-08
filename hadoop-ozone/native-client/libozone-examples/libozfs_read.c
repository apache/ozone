#include "ozfs.h"
#include <stdio.h>
#include <stdlib.h>


int main(int argc, char **argv) {
    ozfsFS fs;
    const char *rfile = argv[1];
    tSize bufferSize = strtoul(argv[3], NULL, 10);
    const char *host = argv[4];
    tPort port = atoi(argv[5]);
    const char *bucket = argv[6];
    const char *volume = argv[7];
    ozfsFile readFile;
    char* buffer;
    tSize curSize;

    if (argc ! = 8) {
        fprintf(stderr, "Usage: ozfs_read <filename> <filesize> <buffersize> <host-name> <port> <bucket-name> <volume-name>\n");
        exit(-1);
    }
    fs = ozfsConnect(host, port, bucket, volume);
    if (!fs) {
        fprintf(stderr, "Oops! Failed to connect to ozfs!\n");
        exit(-1);
    }
    readFile = ozfsOpenFile(fs, rfile, O_RDONLY, bufferSize, 0, 0);
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
        curSize = ozfsRead(fs, readFile, (void*)buffer, curSize);
    }
    free(buffer);
    ozfsCloseFile(fs, readFile);
    ozfsDisconnect(fs);
    return 0;
}
