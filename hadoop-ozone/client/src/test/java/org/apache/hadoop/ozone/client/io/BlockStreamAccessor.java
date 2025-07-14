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

package org.apache.hadoop.ozone.client.io;

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenIdentifier;
import org.apache.hadoop.security.token.Token;

/**
 * An accessor helper class to BlockOutputStreamEntry for test.
 */
public class BlockStreamAccessor {

  private final BlockOutputStreamEntry streamEntry;

  public BlockStreamAccessor(BlockOutputStreamEntry entry) {
    this.streamEntry = entry;
  }

  public BlockID getStreamBlockID() {
    return streamEntry.getBlockID();
  }

  public Pipeline getStreamPipeline() {
    return streamEntry.getPipeline();
  }

  public Token<OzoneBlockTokenIdentifier> getStreamToken() {
    return streamEntry.getToken();
  }

  public long getStreamCurrentPosition() {
    return streamEntry.getCurrentPosition();
  }
}
