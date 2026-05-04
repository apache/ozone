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
 * See the LICENSE file distributed with this work for additional
 * information regarding copyright ownership.
 */

package org.apache.hadoop.ozone.s3;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.junit.jupiter.api.Test;

/**
 * Tests {@link OzoneClientProducer}.
 */
public class TestOzoneClientProducer {

  @Test
  public void destroyClearsThreadLocalS3RequestContext() throws Exception {
    ClientProtocol clientProtocol = mock(ClientProtocol.class);
    ObjectStore objectStore = mock(ObjectStore.class);
    when(objectStore.getClientProxy()).thenReturn(clientProtocol);
    OzoneClient client = mock(OzoneClient.class);
    when(client.getObjectStore()).thenReturn(objectStore);

    OzoneClientProducer producer = new OzoneClientProducer();
    setClient(producer, client);

    producer.destroy();

    verify(clientProtocol).clearThreadLocalS3Auth();
    verify(clientProtocol).clearThreadLocalReadConsistency();
  }

  private static void setClient(OzoneClientProducer producer, OzoneClient client)
      throws Exception {
    Field field = OzoneClientProducer.class.getDeclaredField("client");
    field.setAccessible(true);
    field.set(producer, client);
  }
}
