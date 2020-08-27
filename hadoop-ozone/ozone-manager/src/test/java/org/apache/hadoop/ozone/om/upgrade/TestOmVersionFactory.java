/**
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

package org.apache.hadoop.ozone.om.upgrade;

import static org.apache.hadoop.ozone.om.upgrade.OMLayoutFeatureCatalog.OMLayoutFeature.NEW_FEATURE;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type.CreateKey;

import java.util.function.Function;

import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.key.OMKeyCreateRequest;
import org.apache.hadoop.ozone.om.request.key.OMKeyCreateRequestV2;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.LayoutVersion;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.upgrade.VersionFactoryKey;
import org.apache.hadoop.ozone.upgrade.VersionedInstanceSupplierFactory;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test OmVersionFactory.
 */
public class TestOmVersionFactory {

  @Test
  public void testOmVersionFactory() {
    VersionedInstanceSupplierFactory<OMRequest, OMClientRequest>
        requestFactory = new OmVersionFactory().getRequestFactory();

    Function<OMRequest, ? extends OMClientRequest> instance =
        requestFactory.getInstance(new VersionFactoryKey.Builder()
                .key(CreateKey.name())
                .version(NEW_FEATURE.layoutVersion())
                .build());

    OMRequest omRequest = OMRequest.newBuilder()
        .setLayoutVersion(LayoutVersion.newBuilder()
            .setVersion(NEW_FEATURE.layoutVersion()).build())
        .setCmdType(CreateKey)
        .setClientId("c1")
        .build();
    OMClientRequest clientRequest = instance.apply(omRequest);
    Assert.assertTrue(clientRequest instanceof OMKeyCreateRequestV2);

    instance = requestFactory.getInstance(
        new VersionFactoryKey.Builder().key(CreateKey.name()).build());

    omRequest = OMRequest
        .newBuilder()
        .setCmdType(CreateKey)
        .setClientId("c1")
        .build();
    clientRequest = instance.apply(omRequest);
    Assert.assertTrue(clientRequest instanceof OMKeyCreateRequest);
  }

}