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

package org.apache.hadoop.ozone.om;

import static org.apache.hadoop.hdds.HddsUtils.getScmAddressForClients;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.om.helpers.ServiceInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ServicePort;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.ozone.test.NonHATests;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * This class is to test the REST interface exposed by OzoneManager.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class TestOzoneManagerRestInterface implements NonHATests.TestCase {

  private MiniOzoneCluster cluster;
  private OzoneConfiguration conf;

  @BeforeAll
  void setup() {
    conf = cluster().getConf();
    cluster = cluster();
  }

  @Test
  public void testGetServiceList() throws Exception {
    OzoneManagerHttpServer server =
        cluster.getOzoneManager().getHttpServer();
    HttpClient client = HttpClients.createDefault();
    String connectionUri = "http://" +
        NetUtils.getHostPortString(server.getHttpAddress());
    HttpGet httpGet = new HttpGet(connectionUri + "/serviceList");
    HttpResponse response = client.execute(httpGet);
    String serviceListJson = EntityUtils.toString(response.getEntity());

    ObjectMapper objectMapper = new ObjectMapper();
    TypeReference<List<ServiceInfo>> serviceInfoReference =
        new TypeReference<List<ServiceInfo>>() { };
    List<ServiceInfo> serviceInfos = objectMapper.readValue(
        serviceListJson, serviceInfoReference);
    Map<HddsProtos.NodeType, ServiceInfo> serviceMap = new HashMap<>();
    for (ServiceInfo serviceInfo : serviceInfos) {
      serviceMap.put(serviceInfo.getNodeType(), serviceInfo);
    }

    String omAddress = OmUtils.getOmRpcAddress(conf);
    ServiceInfo omInfo = serviceMap.get(HddsProtos.NodeType.OM);

    assertEquals(omAddress, omInfo.getHostname() + ":" + omInfo.getPort(ServicePort.Type.RPC));
    assertEquals(server.getHttpAddress().getPort(),
        omInfo.getPort(ServicePort.Type.HTTP));

    assertTrue(getScmAddressForClients(conf).iterator().hasNext());
    InetSocketAddress scmAddress =
        getScmAddressForClients(conf).iterator().next();
    ServiceInfo scmInfo = serviceMap.get(HddsProtos.NodeType.SCM);

    assertEquals(scmAddress.getHostName(), scmInfo.getHostname());
    assertEquals(scmAddress.getPort(),
        scmInfo.getPort(ServicePort.Type.RPC));
  }

}
