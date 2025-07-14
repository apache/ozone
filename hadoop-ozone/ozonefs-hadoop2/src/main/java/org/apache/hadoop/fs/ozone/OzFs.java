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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DelegateToFileSystem;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.apache.hadoop.ozone.OzoneConsts;

/**
 * ozone implementation of AbstractFileSystem.
 * This impl delegates to the OzoneFileSystem
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class OzFs extends DelegateToFileSystem {

  public OzFs(URI theUri, Configuration conf)
      throws IOException, URISyntaxException {
    super(theUri, new OzoneFileSystem(), conf,
        OzoneConsts.OZONE_URI_SCHEME, false);
  }

  @Override
  public int getUriDefaultPort() {
    return -1;
  }

  /**
   * Close the file system; the FileContext API doesn't have an explicit close.
   */
  @Override
  protected void finalize() throws Throwable {
    fsImpl.close();
    super.finalize();
  }
}
