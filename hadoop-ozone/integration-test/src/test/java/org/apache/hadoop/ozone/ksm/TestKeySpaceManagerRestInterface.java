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

package org.apache.hadoop.ozone.ksm;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.ksm.helpers.ServiceInfo;
import org.apache.hadoop.ozone.protocol.proto
    .KeySpaceManagerProtocolProtos.ServicePort;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hdds.HddsUtils.getScmAddressForClients;
import static org.apache.hadoop.ozone.KsmUtils.getKsmAddressForClients;

/**
 * This class is to test the REST interface exposed by KeySpaceManager.
 */
public class TestKeySpaceManagerRestInterface {

  private static MiniOzoneCluster cluster;
  private static OzoneConfiguration conf;

  @BeforeClass
  public static void setUp() throws Exception {
    conf = new OzoneConfiguration();
    cluster = MiniOzoneCluster.newBuilder(conf).build();
    cluster.waitForClusterToBeReady();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testGetServiceList() throws Exception {
    KeySpaceManagerHttpServer server =
        cluster.getKeySpaceManager().getHttpServer();
    HttpClient client = HttpClients.createDefault();
    String connectionUri = "http://" +
        NetUtils.getHostPortString(server.getHttpAddress());
    HttpGet httpGet = new HttpGet(connectionUri + "/serviceList");
    HttpResponse response = client.execute(httpGet);
    String serviceListJson = EntityUtils.toString(response.getEntity());

    ObjectMapper objectMapper = new ObjectMapper();
    TypeReference<List<ServiceInfo>> serviceInfoReference =
        new TypeReference<List<ServiceInfo>>() {};
    List<ServiceInfo> serviceInfos = objectMapper.readValue(
        serviceListJson, serviceInfoReference);
    Map<HddsProtos.NodeType, ServiceInfo> serviceMap = new HashMap<>();
    for (ServiceInfo serviceInfo : serviceInfos) {
      serviceMap.put(serviceInfo.getNodeType(), serviceInfo);
    }

    InetSocketAddress ksmAddress =
        getKsmAddressForClients(conf);
    ServiceInfo ksmInfo = serviceMap.get(HddsProtos.NodeType.KSM);

    Assert.assertEquals(ksmAddress.getHostName(), ksmInfo.getHostname());
    Assert.assertEquals(ksmAddress.getPort(),
        ksmInfo.getPort(ServicePort.Type.RPC));
    Assert.assertEquals(server.getHttpAddress().getPort(),
        ksmInfo.getPort(ServicePort.Type.HTTP));

    InetSocketAddress scmAddress =
        getScmAddressForClients(conf);
    ServiceInfo scmInfo = serviceMap.get(HddsProtos.NodeType.SCM);

    Assert.assertEquals(scmAddress.getHostName(), scmInfo.getHostname());
    Assert.assertEquals(scmAddress.getPort(),
        scmInfo.getPort(ServicePort.Type.RPC));

    ServiceInfo datanodeInfo = serviceMap.get(HddsProtos.NodeType.DATANODE);
    DatanodeDetails datanodeDetails = cluster.getHddsDatanodes().get(0)
        .getDatanodeDetails();
    Assert.assertEquals(datanodeDetails.getHostName(),
        datanodeInfo.getHostname());

    Map<ServicePort.Type, Integer> ports = datanodeInfo.getPorts();
    for(ServicePort.Type type : ports.keySet()) {
      switch (type) {
      case HTTP:
      case HTTPS:
        Assert.assertEquals(
            datanodeDetails.getPort(DatanodeDetails.Port.Name.REST).getValue(),
            ports.get(type));
        break;
      default:
        // KSM only sends Datanode's info port details
        // i.e. HTTP or HTTPS
        // Other ports are not expected as of now.
        Assert.fail();
        break;
      }
    }
  }

}
