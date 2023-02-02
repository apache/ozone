/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs.ozone;

import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.fs.impl.StoreImplementationUtils;
import org.apache.hadoop.ozone.client.io.ECKeyOutputStream;
import org.apache.hadoop.ozone.client.io.KeyOutputStream;
import org.apache.hadoop.util.StringUtils;

import java.io.OutputStream;

/**
 * This class is used to workaround Hadoop2 compatibility issues.
 *
 * Hadoop 2 does not support StreamCapabilities, so we create different modules
 * for Hadoop2 and Hadoop3 profiles.
 *
 * The OzoneFileSystem and RootedOzoneFileSystem in Hadoop3 profile uses
 * CapableOzoneFSOutputStream which implements StreamCapabilities interface,
 * whereas the ones in Hadoop2 profile does not.
 */
public class CapableOzoneFSOutputStream  extends OzoneFSOutputStream
    implements StreamCapabilities {
  public CapableOzoneFSOutputStream(OzoneFSOutputStream outputStream) {
    super(outputStream.getWrappedOutputStream());
  }

  @Override
  public boolean hasCapability(String capability) {
    OutputStream os = getWrappedOutputStream().getOutputStream();
    if (os instanceof ECKeyOutputStream) {
      return false;
    } else if (os instanceof KeyOutputStream) {
      switch (StringUtils.toLowerCase(capability)) {
      case StreamCapabilities.HFLUSH:
      case StreamCapabilities.HSYNC:
        return true;
      default:
        return false;
      }
    }
    // deal with CryptoOutputStream
    return StoreImplementationUtils.hasCapability(os, capability);
  }
}
