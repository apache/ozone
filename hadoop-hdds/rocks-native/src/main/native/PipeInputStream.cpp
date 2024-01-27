/*
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

#include <unistd.h>
#include <iostream>
#include "Pipe.h"
#include "cplusplus_to_java_convert.h"
#include "org_apache_hadoop_hdds_utils_db_managed_PipeInputStream.h"


jlong Java_org_apache_hadoop_hdds_utils_db_managed_PipeInputStream_newPipe(JNIEnv *, jobject) {
    Pipe *pipe = new Pipe();
    return GET_CPLUSPLUS_POINTER(pipe);
}

jint Java_org_apache_hadoop_hdds_utils_db_managed_PipeInputStream_readInternal(JNIEnv *env, jobject object, jbyteArray jbyteArray, jint capacity, jlong nativeHandle) {
    int cap_int = capacity;
    Pipe *pipe = reinterpret_cast<Pipe *>(nativeHandle);
    jbyte *b = (env)->GetByteArrayElements(jbyteArray, JNI_FALSE);
    cap_int = read(pipe->getReadFd(), b, cap_int);
    if (cap_int == 0) {
        if (!pipe->isOpen()) {
            cap_int = -1;
        }
    }
    env->ReleaseByteArrayElements(jbyteArray, b, 0);
    return cap_int;
}

void Java_org_apache_hadoop_hdds_utils_db_managed_PipeInputStream_closeInternal(JNIEnv *env, jobject object, jlong nativeHandle) {
    delete reinterpret_cast<Pipe *>(nativeHandle);
}

