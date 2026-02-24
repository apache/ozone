/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.ha.io;

import com.google.common.primitives.Longs;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.UnsafeByteOperations;

/**
 * {@link ScmCodec} for {@code Long} objects.
 */
public class ScmLongCodec implements ScmCodec<Long> {

  @Override
  public ByteString serialize(Long object) {
    // toByteArray returns a new array
    return UnsafeByteOperations.unsafeWrap(Longs.toByteArray(object));
  }

  @Override
  public Long deserialize(Class<?> type, ByteString value) {
    return Longs.fromByteArray(value.toByteArray());
  }

}
