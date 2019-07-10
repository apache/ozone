/**
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

package org.apache.hadoop.fs.ozone;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DelegateToFileSystem;
import org.apache.hadoop.ozone.OzoneConsts;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * ozone implementation of AbstractFileSystem.
 * This impl delegates to the OzoneFileSystem
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class BasicOzFs extends DelegateToFileSystem {

  public BasicOzFs(URI theUri, Configuration conf)
      throws IOException, URISyntaxException {
    super(theUri, new BasicOzoneFileSystem(), conf,
        OzoneConsts.OZONE_URI_SCHEME, false);
  }

}
