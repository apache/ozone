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

#include "org_apache_hadoop_hdds_utils_db_managed_ManagedSSTDumpTool.h"
#include "rocksdb/options.h"
#include "rocksdb/sst_dump_tool.h"
#include <string>
#include "cplusplus_to_java_convert.h"
#include "Pipe.h"
#include <iostream>

jint Java_org_apache_hadoop_hdds_utils_db_managed_ManagedSSTDumpTool_runInternal(JNIEnv *env, jobject obj,
  jobjectArray argsArray, jlong optionsHandle, jlong pipeHandle) {
    ROCKSDB_NAMESPACE::SSTDumpTool dumpTool;
    ROCKSDB_NAMESPACE::Options options;
    Pipe *pipe = reinterpret_cast<Pipe *>(pipeHandle);
    int length = env->GetArrayLength(argsArray);
    char *args[length + 1];
    for (int i = 0; i < length; i++) {
        jstring str_val = (jstring)env->GetObjectArrayElement(argsArray, (jsize)i);
        char *utf_str = (char *)env->GetStringUTFChars(str_val, JNI_FALSE);
        args[i + 1] = utf_str;
    }
    FILE *wr = fdopen(pipe->getWriteFd(), "w");
    int ret = dumpTool.Run(length + 1, args, options, wr);
    for (int i = 1; i < length + 1; i++) {
        jstring str_val = (jstring)env->GetObjectArrayElement(argsArray, (jsize)(i - 1));
        env->ReleaseStringUTFChars(str_val, args[i]);
    }
    fclose(wr);
    pipe->close();
    return ret;
}
