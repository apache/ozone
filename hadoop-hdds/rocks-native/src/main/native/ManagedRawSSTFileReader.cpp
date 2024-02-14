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

#include "org_apache_hadoop_hdds_utils_db_managed_ManagedRawSSTFileReader.h"
#include "rocksdb/options.h"
#include "rocksdb/raw_sst_file_reader.h"
#include "rocksdb/raw_iterator.h"
#include <string>
#include "cplusplus_to_java_convert.h"
#include <iostream>

jlong Java_org_apache_hadoop_hdds_utils_db_managed_ManagedRawSSTFileReader_newRawSSTFileReader(JNIEnv *env, jobject obj,
                                                                                              jlong optionsHandle,
                                                                                              jstring jfilename,
                                                                                              jint readahead_size) {
    ROCKSDB_NAMESPACE::Options *options = reinterpret_cast<ROCKSDB_NAMESPACE::Options *>(optionsHandle);
    const char *file_path = env->GetStringUTFChars(jfilename, nullptr);
    size_t readAheadSizeValue = static_cast<size_t>(readahead_size);
    ROCKSDB_NAMESPACE::RawSstFileReader* rawSstFileReader =
            new ROCKSDB_NAMESPACE::RawSstFileReader(*options, file_path, readAheadSizeValue, true, true);
    env->ReleaseStringUTFChars(jfilename, file_path);
    return GET_CPLUSPLUS_POINTER(rawSstFileReader);
}

jlong Java_org_apache_hadoop_hdds_utils_db_managed_ManagedRawSSTFileReader_newIterator(JNIEnv *env, jobject obj,
                                                                                       jlong native_handle,
                                                                                       jboolean jhas_from,
                                                                                       jlong fromSliceHandle,
                                                                                       jboolean jhas_to,
                                                                                       jlong toSliceHandle) {
    ROCKSDB_NAMESPACE::Slice* fromSlice = nullptr;
    ROCKSDB_NAMESPACE::Slice* toSlice = nullptr;
    ROCKSDB_NAMESPACE::RawSstFileReader* rawSstFileReader =
            reinterpret_cast<ROCKSDB_NAMESPACE::RawSstFileReader*>(native_handle);
    bool has_from = static_cast<bool>(jhas_from);
    bool has_to = static_cast<bool>(jhas_to);
    if (has_from) {
        fromSlice = reinterpret_cast<ROCKSDB_NAMESPACE::Slice*>(fromSliceHandle);
    }
    if (has_to) {
        toSlice = reinterpret_cast<ROCKSDB_NAMESPACE::Slice*>(toSliceHandle);
    }
    ROCKSDB_NAMESPACE::RawIterator* iterator = rawSstFileReader->newIterator(has_from, fromSlice, has_to, toSlice);
    return GET_CPLUSPLUS_POINTER(iterator);
}

void Java_org_apache_hadoop_hdds_utils_db_managed_ManagedRawSSTFileReader_disposeInternal(JNIEnv *env, jobject obj,
                                                                                          jlong native_handle) {
    delete reinterpret_cast<ROCKSDB_NAMESPACE::RawSstFileReader*>(native_handle);
}
