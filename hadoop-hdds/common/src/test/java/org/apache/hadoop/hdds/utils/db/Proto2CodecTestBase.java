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

package org.apache.hadoop.hdds.utils.db;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.protobuf.InvalidProtocolBufferException;
import org.junit.jupiter.api.Test;

/**
 * Test {@link Proto2Codec} related classes.
 */
public abstract class Proto2CodecTestBase<T> {
  public abstract Codec<T> getCodec();

  @Test
  public void testInvalidProtocolBuffer() {
    final CodecException exception =
        assertThrows(CodecException.class,
            () -> getCodec().fromPersistedFormat("random".getBytes(UTF_8)));
    final InvalidProtocolBufferException cause = assertInstanceOf(
        InvalidProtocolBufferException.class, exception.getCause());
    assertThat(cause.getMessage())
        .contains("the input ended unexpectedly");
  }

  @Test
  public void testFromPersistedFormat() {
    assertThrows(NullPointerException.class,
        () -> getCodec().fromPersistedFormat(null));
  }

  @Test
  public void testToPersistedFormat() throws Exception {
    assertThrows(NullPointerException.class,
        () -> getCodec().toPersistedFormat(null));
  }
}
