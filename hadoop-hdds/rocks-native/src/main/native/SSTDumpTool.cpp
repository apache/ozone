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
#include "string"

void Java_org_apache_hadoop_hdds_utils_db_managed_ManagedSSTDumpTool_runInternal(JNIEnv *env, jobject obj, jobjectArray argsArray) {
    ROCKSDB_NAMESPACE::SSTDumpTool dumpTool;
    int length = env->GetArrayLength(argsArray);
    const char* args[length + 1];
    args[0] = strdup("./sst_dump");
    for(int i = 0; i < env->GetArrayLength(argsArray); i++) {

        args[i+1] = (char*)env->GetStringUTFChars((jstring)env->
                GetObjectArrayElement(argsArray, (jsize)i), JNI_FALSE);
    }
    dumpTool.Run(length + 1, args);
}