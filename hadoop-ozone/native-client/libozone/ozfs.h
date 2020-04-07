#ifndef OZFS_DOT_H    
#define OZFS_DOT_H

#include "hdfs/hdfs.h"

struct hdfs_internal;
typedef struct hdfs_internal* ozfsFS;

struct hdfsFile_internal;
typedef struct hdfsFile_internal* ozfsFile;

ozfsFS ozfsConnect(const char* nn, tPort port, const char* bucket, const char* volume);

ozfsFile ozfsOpenFile(ozfsFS fs, const char *path, int flags, int bufferSize, short replication, tSize blockSize);

tSize ozfsRead(ozfsFS fs, ozfsFile f, void* buffer, tSize length);

int ozfsCloseFile(ozfsFS fs, ozfsFile file);

int ozfsDisconnect(ozfsFS fs);

tSize ozfsWrite(ozfsFS fs, ozfsFile f, const void* buffer, tSize length);

#endif

