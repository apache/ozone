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

package org.apache.hadoop.ozone;

import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.FsServerDefaultsProto;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.FsServerDefaultsProto.Builder;

/** Provides server default configuration values to clients. */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class OzoneFsServerDefaults extends FsServerDefaults {

  public OzoneFsServerDefaults() {
  }

  public OzoneFsServerDefaults(String keyProviderUri) {
    super(0L, 16 * 1024, 0, (short)0, 0, false, 0L, null, keyProviderUri);
  }

  public FsServerDefaultsProto getProtobuf() {
    Builder builder = FsServerDefaultsProto.newBuilder();
    if (getKeyProviderUri() != null) {
      builder.setKeyProviderUri(getKeyProviderUri());
    }
    return builder.build();
  }

  public static OzoneFsServerDefaults getFromProtobuf(
      FsServerDefaultsProto serverDefaults) {
    String keyProviderUri = null;
    if (serverDefaults.hasKeyProviderUri()) {
      keyProviderUri = serverDefaults.getKeyProviderUri();
    }
    return new OzoneFsServerDefaults(keyProviderUri);
  }
}
