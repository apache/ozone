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

package org.apache.hadoop.ozone.client;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.s3.metrics.S3GatewayMetrics;

/**
 * In-memory OzoneClient for testing.
 */
public class OzoneClientStub extends OzoneClient {
  public OzoneClientStub() {
    this(new ObjectStoreStub());
  }

  public OzoneClientStub(ObjectStoreStub objectStoreStub) {
    super(objectStoreStub, new ClientProtocolStub(objectStoreStub));
    S3GatewayMetrics.create(new OzoneConfiguration());
  }

  @Override
  public void close() {
    //NOOP.
  }

}
