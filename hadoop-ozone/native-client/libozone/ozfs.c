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
    char string[len+2];
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

