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

#include "org_apache_hadoop_hdds_utils_db_ManagedRawSSTFileIterator.h"
#include "rocksdb/options.h"
#include "rocksdb/raw_iterator.h"
#include <string>
#include "cplusplus_to_java_convert.h"
#include <iostream>

template <class T>
static jint copyToDirect(JNIEnv* env, T& source, jobject jtarget, jint jtarget_off, jint jtarget_len);

jboolean Java_org_apache_hadoop_hdds_utils_db_ManagedRawSSTFileIterator_hasNext(JNIEnv *env, jobject obj,
                                                                                jlong native_handle) {
    return static_cast<jboolean>(reinterpret_cast<ROCKSDB_NAMESPACE::RawIterator*>(native_handle)->Valid());
}

void Java_org_apache_hadoop_hdds_utils_db_ManagedRawSSTFileIterator_next(JNIEnv *env, jobject obj,
                                                                         jlong native_handle) {
    reinterpret_cast<ROCKSDB_NAMESPACE::RawIterator*>(native_handle)->Next();
}

jint Java_org_apache_hadoop_hdds_utils_db_ManagedRawSSTFileIterator_getKey(JNIEnv *env,
                                                                           jobject obj,
                                                                           jlong native_handle,
                                                                           jobject jtarget,
                                                                           jint jtarget_off, jint jtarget_len) {
    ROCKSDB_NAMESPACE::Slice slice = reinterpret_cast<ROCKSDB_NAMESPACE::RawIterator*>(native_handle)->key();
    return copyToDirect(env, slice, jtarget, jtarget_off, jtarget_len);
}


jint Java_org_apache_hadoop_hdds_utils_db_ManagedRawSSTFileIterator_getValue(JNIEnv *env,
                                                                             jobject obj,
                                                                             jlong native_handle,
                                                                             jobject jtarget,
                                                                             jint jtarget_off, jint jtarget_len) {
    ROCKSDB_NAMESPACE::Slice slice = reinterpret_cast<ROCKSDB_NAMESPACE::RawIterator*>(native_handle)->value();
    return copyToDirect(env, slice, jtarget, jtarget_off, jtarget_len);
}

jlong Java_org_apache_hadoop_hdds_utils_db_ManagedRawSSTFileIterator_getSequenceNumber(JNIEnv *env,
                                                                                       jobject obj,
                                                                                       jlong native_handle) {
    uint64_t sequence_number = reinterpret_cast<ROCKSDB_NAMESPACE::RawIterator*>(native_handle)->sequenceNumber();
    jlong result;
    std::memcpy(&result, &sequence_number, sizeof(jlong));
    return result;
}


jint Java_org_apache_hadoop_hdds_utils_db_ManagedRawSSTFileIterator_getType(JNIEnv *env,
                                                                            jobject obj,
                                                                            jlong native_handle) {
    uint32_t type = reinterpret_cast<ROCKSDB_NAMESPACE::RawIterator*>(native_handle)->type();
    return static_cast<jint>(type);
}


void Java_org_apache_hadoop_hdds_utils_db_ManagedRawSSTFileIterator_closeInternal(JNIEnv *env,
                                                                                  jobject obj,
                                                                                  jlong native_handle) {
    delete reinterpret_cast<ROCKSDB_NAMESPACE::RawIterator*>(native_handle);
}

template <class T>
static jint copyToDirect(JNIEnv* env, T& source, jobject jtarget,
                         jint jtarget_off, jint jtarget_len) {
  char* target = reinterpret_cast<char*>(env->GetDirectBufferAddress(jtarget));
  if (target == nullptr || env->GetDirectBufferCapacity(jtarget) < (jtarget_off + jtarget_len)) {
    jclass exClass = env->FindClass("java/lang/IllegalArgumentException");
    if (exClass != nullptr) {
        env->ThrowNew(exClass, "Invalid buffer address or capacity");
    }
    return -1;
  }

  target += jtarget_off;

  const jint cvalue_len = static_cast<jint>(source.size());
  const jint length = std::min(jtarget_len, cvalue_len);

  memcpy(target, source.data(), length);

  return cvalue_len;
}
