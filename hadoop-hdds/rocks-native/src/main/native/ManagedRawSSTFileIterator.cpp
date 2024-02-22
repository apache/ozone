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

#include "org_apache_hadoop_hdds_utils_db_managed_ManagedRawSSTFileIterator.h"
#include "rocksdb/options.h"
#include "rocksdb/raw_iterator.h"
#include <string>
#include "cplusplus_to_java_convert.h"
#include <iostream>

jboolean Java_org_apache_hadoop_hdds_utils_db_managed_ManagedRawSSTFileIterator_hasNext(JNIEnv *env, jobject obj,
                                                                                           jlong native_handle) {
    return static_cast<jboolean>(reinterpret_cast<ROCKSDB_NAMESPACE::RawIterator*>(native_handle)->Valid());
}

void Java_org_apache_hadoop_hdds_utils_db_managed_ManagedRawSSTFileIterator_next(JNIEnv *env, jobject obj,
                                                                                       jlong native_handle) {
    reinterpret_cast<ROCKSDB_NAMESPACE::RawIterator*>(native_handle)->Next();
}

jbyteArray Java_org_apache_hadoop_hdds_utils_db_managed_ManagedRawSSTFileIterator_getKey(JNIEnv *env,
                                                                                               jobject obj,
                                                                                               jlong native_handle) {
    ROCKSDB_NAMESPACE::Slice slice = reinterpret_cast<ROCKSDB_NAMESPACE::RawIterator*>(native_handle)->key();
    jbyteArray jkey = env->NewByteArray(static_cast<jsize>(slice.size()));
    if (jkey == nullptr) {
        // exception thrown: OutOfMemoryError
        return nullptr;
    }
    env->SetByteArrayRegion(
            jkey, 0, static_cast<jsize>(slice.size()),
            const_cast<jbyte*>(reinterpret_cast<const jbyte*>(slice.data())));
    return jkey;
}


jbyteArray Java_org_apache_hadoop_hdds_utils_db_managed_ManagedRawSSTFileIterator_getValue(JNIEnv *env,
                                                                                               jobject obj,
                                                                                               jlong native_handle) {
    ROCKSDB_NAMESPACE::Slice slice = reinterpret_cast<ROCKSDB_NAMESPACE::RawIterator*>(native_handle)->value();
    jbyteArray jkey = env->NewByteArray(static_cast<jsize>(slice.size()));
    if (jkey == nullptr) {
        // exception thrown: OutOfMemoryError
        return nullptr;
    }
    env->SetByteArrayRegion(
            jkey, 0, static_cast<jsize>(slice.size()),
            const_cast<jbyte*>(reinterpret_cast<const jbyte*>(slice.data())));
    return jkey;
}

jlong Java_org_apache_hadoop_hdds_utils_db_managed_ManagedRawSSTFileIterator_getSequenceNumber(JNIEnv *env,
                                                                                                     jobject obj,
                                                                                                     jlong native_handle) {
    uint64_t sequence_number =
            reinterpret_cast<ROCKSDB_NAMESPACE::RawIterator*>(native_handle)->sequenceNumber();
    jlong result;
    std::memcpy(&result, &sequence_number, sizeof(jlong));
    return result;
}


jint Java_org_apache_hadoop_hdds_utils_db_managed_ManagedRawSSTFileIterator_getType(JNIEnv *env,
                                                                                          jobject obj,
                                                                                          jlong native_handle) {
    uint32_t type = reinterpret_cast<ROCKSDB_NAMESPACE::RawIterator*>(native_handle)->type();
    return static_cast<jint>(type);
}


void Java_org_apache_hadoop_hdds_utils_db_managed_ManagedRawSSTFileIterator_closeInternal(JNIEnv *env,
                                                                                                jobject obj,
                                                                                                jlong native_handle) {
    delete reinterpret_cast<ROCKSDB_NAMESPACE::RawIterator*>(native_handle);
}
