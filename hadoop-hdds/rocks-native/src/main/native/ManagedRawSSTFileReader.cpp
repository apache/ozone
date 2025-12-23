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

#include "org_apache_hadoop_hdds_utils_db_ManagedRawSSTFileReader.h"
#include "rocksdb/options.h"
#include "rocksdb/raw_sst_file_reader.h"
#include "rocksdb/raw_iterator.h"
#include <string>
#include "cplusplus_to_java_convert.h"
#include <iostream>

jlong Java_org_apache_hadoop_hdds_utils_db_ManagedRawSSTFileReader_newRawSSTFileReader(JNIEnv *env, jobject obj,
                                                                                               jlong options_handle,
                                                                                               jstring jfilename,
                                                                                               jint readahead_size) {
    ROCKSDB_NAMESPACE::Options *options = reinterpret_cast<ROCKSDB_NAMESPACE::Options *>(options_handle);
    const char *file_path = env->GetStringUTFChars(jfilename, nullptr);
    size_t read_ahead_size_value = static_cast<size_t>(readahead_size);
    ROCKSDB_NAMESPACE::RawSstFileReader* raw_sst_file_reader =
            new ROCKSDB_NAMESPACE::RawSstFileReader(*options, file_path, read_ahead_size_value, true, true);
    env->ReleaseStringUTFChars(jfilename, file_path);
    return GET_CPLUSPLUS_POINTER(raw_sst_file_reader);
}

jlong Java_org_apache_hadoop_hdds_utils_db_ManagedRawSSTFileReader_newIterator(JNIEnv *env, jobject obj,
                                                                                       jlong native_handle,
                                                                                       jboolean jhas_from,
                                                                                       jlong from_slice_handle,
                                                                                       jboolean jhas_to,
                                                                                       jlong to_slice_handle) {
    ROCKSDB_NAMESPACE::Slice* from_slice = nullptr;
    ROCKSDB_NAMESPACE::Slice* to_slice = nullptr;
    ROCKSDB_NAMESPACE::RawSstFileReader* raw_sst_file_reader =
            reinterpret_cast<ROCKSDB_NAMESPACE::RawSstFileReader*>(native_handle);
    bool has_from = static_cast<bool>(jhas_from);
    bool has_to = static_cast<bool>(jhas_to);
    if (has_from) {
        from_slice = reinterpret_cast<ROCKSDB_NAMESPACE::Slice*>(from_slice_handle);
    }
    if (has_to) {
        to_slice = reinterpret_cast<ROCKSDB_NAMESPACE::Slice*>(to_slice_handle);
    }
    ROCKSDB_NAMESPACE::RawIterator* iterator = raw_sst_file_reader->newIterator(has_from, from_slice, has_to, to_slice);
    return GET_CPLUSPLUS_POINTER(iterator);
}

void Java_org_apache_hadoop_hdds_utils_db_ManagedRawSSTFileReader_disposeInternal(JNIEnv *env, jobject obj,
                                                                                          jlong native_handle) {
    delete reinterpret_cast<ROCKSDB_NAMESPACE::RawSstFileReader*>(native_handle);
}
