//
// Created by Swaminathan Balachandran on 3/2/23.
//
#include <unistd.h>
#include <iostream>
//#include <stdio.h>
#include "Pipe.h"
#include "cplusplus_to_java_convert.h"
#include "org_apache_hadoop_hdds_utils_db_managed_PipeInputStream.h"


jlong Java_org_apache_hadoop_hdds_utils_db_managed_PipeInputStream_newPipe(JNIEnv *, jobject) {
    Pipe* pipe = new Pipe();
    return GET_CPLUSPLUS_POINTER(pipe);
}

jint Java_org_apache_hadoop_hdds_utils_db_managed_PipeInputStream_readInternal(JNIEnv *env, jobject object, jbyteArray jbyteArray, jint capacity, jlong nativeHandle) {
    int cap_int = capacity;
    Pipe* pipe = reinterpret_cast<Pipe*>(nativeHandle);
    jbyte* b = (env)->GetByteArrayElements(jbyteArray, JNI_FALSE);
    cap_int = read(pipe->getReadFd(), b, cap_int);
    if (cap_int == 0) {
        if (!pipe->isOpen()) {
            cap_int = -1;
        }
    }
    (env)->ReleaseByteArrayElements(jbyteArray, b, 0);
    return cap_int;
}

void Java_org_apache_hadoop_hdds_utils_db_managed_PipeInputStream_closeInternal(JNIEnv *env, jobject object, jlong nativeHandle) {
    delete reinterpret_cast<Pipe*>(nativeHandle);
}

