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

package org.apache.hadoop.fs.ozone;

import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.hdds.scm.storage.ByteBufferStreamOutput;
import org.apache.hadoop.ozone.client.io.KeyDataStreamOutput;
import org.apache.hadoop.util.StringUtils;

/**
 * This class is used to workaround Hadoop2 compatibility issues.
 *
 * Hadoop 2 does not support StreamCapabilities, so we create different modules
 * for Hadoop2 and Hadoop3 profiles.
 *
 * The OzoneFileSystem and RootedOzoneFileSystem in Hadoop3 profile uses
 * CapableOzoneFSDataStreamOutput which implements StreamCapabilities interface,
 * whereas the ones in Hadoop2 profile does not.
 */
public class CapableOzoneFSDataStreamOutput extends OzoneFSDataStreamOutput
    implements StreamCapabilities {
  private final boolean isHsyncEnabled;

  public CapableOzoneFSDataStreamOutput(OzoneFSDataStreamOutput outputStream,
                                          boolean enabled) {
    super(outputStream.getByteBufferStreamOutput());
    this.isHsyncEnabled = enabled;
  }

  @Override
  public boolean hasCapability(String capability) {
    ByteBufferStreamOutput os = getByteBufferStreamOutput();
    return hasWrappedCapability(os, capability);
  }

  private boolean hasWrappedCapability(ByteBufferStreamOutput os, String capability) {
    if (os instanceof KeyDataStreamOutput) {
      switch (StringUtils.toLowerCase(capability)) {
      case StreamCapabilities.HFLUSH:
      case StreamCapabilities.HSYNC:
        return isHsyncEnabled;
      default:
        return false;
      }
    }
    return false;
  }
}
