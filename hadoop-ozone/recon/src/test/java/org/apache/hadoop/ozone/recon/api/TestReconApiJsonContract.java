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

package org.apache.hadoop.ozone.recon.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.ByteArrayOutputStream;
import java.lang.annotation.Annotation;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedHashMap;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.LayoutVersionProto;
import org.apache.hadoop.hdds.scm.node.DatanodeInfo;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline.PipelineState;
import org.apache.hadoop.ozone.recon.api.types.ClusterStateResponse;
import org.apache.hadoop.ozone.recon.api.types.ClusterStorageReport;
import org.apache.hadoop.ozone.recon.api.types.ContainerMetadata;
import org.apache.hadoop.ozone.recon.api.types.DatanodeMetadata;
import org.apache.hadoop.ozone.recon.api.types.DatanodesResponse;
import org.apache.hadoop.ozone.recon.api.types.KeyMetadata;
import org.apache.hadoop.ozone.recon.api.types.MissingContainerMetadata;
import org.apache.hadoop.ozone.recon.api.types.MissingContainersResponse;
import org.apache.hadoop.ozone.recon.api.types.PipelineMetadata;
import org.apache.hadoop.ozone.recon.api.types.PipelinesResponse;
import org.apache.hadoop.ozone.recon.api.types.RemoveDataNodesResponseWrapper;
import org.apache.hadoop.ozone.recon.api.types.UnhealthyContainerMetadata;
import org.apache.hadoop.ozone.recon.api.types.UnhealthyContainersResponse;
import org.apache.ozone.recon.schema.generated.tables.pojos.UnhealthyContainers;
import org.glassfish.jersey.jackson.internal.jackson.jaxrs.json.JacksonJaxbJsonProvider;
import org.junit.jupiter.api.Test;

/**
 * JSON shape contract for Recon's REST API under {@code /api/v1}.
 *
 * <p>Recon serves JSON through Jersey with {@code jersey-media-json-jackson}, which uses
 * {@link JacksonJaxbJsonProvider}. Many response types in {@code recon.api.types} mix
 * {@link com.fasterxml.jackson.annotation.JsonProperty} with JAXB
 * {@link javax.xml.bind.annotation.XmlElement} names. If those JAXB names stop driving
 * serialization (for example after a Jersey or Jackson upgrade, replacing the provider, or
 * registering a plain {@link ObjectMapper} without JAXB annotation support), JSON property
 * names can switch to JavaBean names. The Recon UI and any scripts that call the API would
 * then see missing or renamed fields without a compile-time failure.
 *
 * <p>These tests pin the behavior we rely on: entities must serialize like the live Recon
 * stack, and known "wrong" keys (bean names where JAXB names are required) must not appear.
 * They complement endpoint tests that only inspect Java objects from {@code Response#getEntity()}
 * and never exercise the wire format.
 */
public class TestReconApiJsonContract {

  private static final JacksonJaxbJsonProvider JERSEY_JSON =
      new JacksonJaxbJsonProvider();

  private static final ObjectMapper TREE_MAPPER = new ObjectMapper();

  private static String toWireJson(Object entity) throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    JERSEY_JSON.writeTo(
        entity,
        entity.getClass(),
        entity.getClass(),
        new Annotation[0],
        MediaType.APPLICATION_JSON_TYPE,
        new MultivaluedHashMap<String, Object>(),
        out);
    return out.toString(StandardCharsets.UTF_8.name());
  }

  private static JsonNode parse(String json) throws Exception {
    return TREE_MAPPER.readTree(json);
  }

  private static void assertHasKeys(JsonNode objectNode, String... keys) {
    for (String key : keys) {
      assertTrue(objectNode.has(key), () -> "Missing expected JSON key: " + key);
    }
  }

  /** Keys that must not appear if JAXB naming is applied (plain Jackson bean names). */
  private static void assertLacksKeys(JsonNode objectNode, String... keys) {
    for (String key : keys) {
      assertFalse(objectNode.has(key),
          () -> "Unexpected JSON key (likely wrong serializer): " + key);
    }
  }

  /**
   * Builds a {@link DatanodeMetadata} with non-null operational state so {@code opState}
   * appears on the wire; {@code opState} is the JAXB name, while the accessor is
   * {@link DatanodeMetadata#getOperationalState()}.
   */
  private static DatanodeMetadata sampleDatanodeMetadataWithOpState() {
    DatanodeInfo datanodeInfo = mock(DatanodeInfo.class);
    when(datanodeInfo.getUuidString()).thenReturn("uuid-opstate-test");
    when(datanodeInfo.getHostName()).thenReturn("dn.jaxb.example");
    when(datanodeInfo.getNetworkLocation()).thenReturn("/default-rack");
    when(datanodeInfo.getPersistedOpState())
        .thenReturn(NodeOperationalState.IN_SERVICE);
    when(datanodeInfo.getVersion()).thenReturn("ver");
    when(datanodeInfo.getRevision()).thenReturn("rev");
    // NON_DEFAULT omits 0 for setupTime on the wire
    when(datanodeInfo.getSetupTime()).thenReturn(1L);
    when(datanodeInfo.getLastKnownLayoutVersion()).thenReturn(
        LayoutVersionProto.newBuilder()
            .setMetadataLayoutVersion(1)
            .setSoftwareLayoutVersion(1)
            .build());

    return DatanodeMetadata.newBuilder()
        .setState(NodeState.HEALTHY)
        .setDatanode(datanodeInfo)
        .setLeaderCount(1)
        .setContainers(1)
        .setOpenContainers(1)
        .setPipelines(Collections.emptyList())
        .build();
  }

  private static UnhealthyContainerMetadata sampleUnhealthyContainerMetadata() {
    UnhealthyContainers rec = new UnhealthyContainers(
        100L,
        "UNDER_REPLICATED",
        1_700_000_000L,
        3,
        1,
        2,
        "recon-json-contract");
    UUID pipelineId = UUID.fromString("33333333-3333-3333-3333-333333333333");
    return new UnhealthyContainerMetadata(rec, Collections.emptyList(), pipelineId,
        42L);
  }

  private static MissingContainerMetadata sampleMissingContainerMetadata() {
    return new MissingContainerMetadata(
        200L,
        1_600_000_000L,
        7L,
        UUID.fromString("44444444-4444-4444-4444-444444444444"),
        Collections.emptyList());
  }

  private static ContainerMetadata sampleContainerMetadata() {
    ContainerMetadata cm = new ContainerMetadata(99L);
    cm.setNumberOfKeys(11L);
    cm.setPipelines(Collections.emptyList());
    return cm;
  }

  @Test
  public void pipelinesResponseJsonShape() throws Exception {
    PipelineMetadata pipeline = PipelineMetadata.newBuilder()
        .setPipelineId(UUID.fromString("11111111-1111-1111-1111-111111111111"))
        .setStatus(PipelineState.OPEN)
        .setDatanodes(Collections.emptyList())
        .setReplicationConfig(
            RatisReplicationConfig.getInstance(ReplicationFactor.ONE))
        .setLeaderNode("leader.example")
        .setContainers(2)
        .setDuration(1000L)
        .setLeaderElections(3L)
        .setLastLeaderElection(500L)
        .build();
    PipelinesResponse body = new PipelinesResponse(1,
        Collections.singletonList(pipeline));

    JsonNode root = parse(toWireJson(body));
    assertEquals(1, root.get("totalCount").asLong());
    assertTrue(root.get("pipelines").isArray());
    assertEquals(1, root.get("pipelines").size());
    JsonNode p0 = root.get("pipelines").get(0);
    assertHasKeys(p0,
        "pipelineId", "status", "leaderNode", "datanodes", "lastLeaderElection",
        "duration", "leaderElections", "replicationType", "replicationFactor",
        "containers");
  }

  @Test
  public void datanodesResponseJsonShape() throws Exception {
    DatanodeMetadata dn = DatanodeMetadata.newBuilder()
        .setHostname("dn1.example")
        .setUuid("abc-def-uuid")
        .setState(NodeState.HEALTHY)
        .setContainers(1)
        .setOpenContainers(1)
        // NON_DEFAULT JSON include omits 0; use 1 so "leaderCount" appears on the wire
        .setLeaderCount(1)
        .setPipelines(Collections.emptyList())
        .build();
    DatanodesResponse body = new DatanodesResponse(
        Collections.singletonList(dn));

    JsonNode root = parse(toWireJson(body));
    assertEquals(1, root.get("totalCount").asLong());
    assertTrue(root.get("datanodes").isArray());
    JsonNode d0 = root.get("datanodes").get(0);
    assertHasKeys(d0,
        "uuid", "hostname", "state", "containers", "openContainers", "leaderCount",
        "pipelines");
  }

  @Test
  public void clusterStateResponseJsonShape() throws Exception {
    ClusterStorageReport storage = ClusterStorageReport.newBuilder()
        .setCapacity(1000L)
        .setUsed(400L)
        .setRemaining(600L)
        .setCommitted(0L)
        .setMinimumFreeSpace(0L)
        .setReserved(0L)
        .setFilesystemCapacity(1000L)
        .setFilesystemUsed(400L)
        .setFilesystemAvailable(600L)
        .build();
    ClusterStateResponse.Builder csb = ClusterStateResponse.newBuilder()
        .setPipelines(2)
        .setTotalDatanodes(5)
        .setHealthyDatanodes(4)
        .setStorageReport(storage)
        .setContainers(10)
        .setMissingContainers(1)
        .setOpenContainers(8)
        .setDeletedContainers(1)
        .setVolumes(3L)
        .setBuckets(7L)
        .setKeys(100L)
        .setScmServiceId("scm-id")
        .setOmServiceId("om-id");
    csb.setKeysPendingDeletion(9L);
    csb.setDeletedDirs(2L);
    ClusterStateResponse body = csb.build();

    JsonNode root = parse(toWireJson(body));
    assertHasKeys(root,
        "pipelines", "totalDatanodes", "healthyDatanodes", "storageReport",
        "containers", "missingContainers", "openContainers", "deletedContainers",
        "volumes", "buckets", "keys", "keysPendingDeletion", "deletedDirs",
        "scmServiceId", "omServiceId");

    JsonNode sr = root.get("storageReport");
    assertHasKeys(sr,
        "capacity", "used", "remaining", "committed", "minimumFreeSpace", "reserved",
        "filesystemCapacity", "filesystemUsed", "filesystemAvailable");
  }

  /**
   * {@link KeyMetadata} (OM DB insight / key listing) uses PascalCase JAXB names
   * ({@code Volume}, {@code Bucket}, …). Recon clients must keep seeing those names.
   */
  @Test
  public void keyMetadataJerseyJsonUsesJaxbXmlElementNames() throws Exception {
    KeyMetadata km = new KeyMetadata();
    km.setVolume("vol-a");
    km.setBucket("bucket-a");
    km.setKey("key-a");
    km.setCompletePath("/vol-a/bucket-a/key-a");
    km.setDataSize(42L);
    km.setCreationTime(Instant.parse("2020-01-01T00:00:00Z"));
    km.setModificationTime(Instant.parse("2020-01-02T00:00:00Z"));
    km.setVersions(Collections.singletonList(1L));

    JsonNode root = parse(toWireJson(km));
    assertEquals("vol-a", root.get("Volume").asText());
    assertEquals("bucket-a", root.get("Bucket").asText());
    assertEquals("key-a", root.get("Key").asText());
    assertEquals("/vol-a/bucket-a/key-a", root.get("CompletePath").asText());
    assertEquals(42L, root.get("DataSize").asLong());
    assertHasKeys(root,
        "Volume", "Bucket", "Key", "CompletePath", "DataSize", "Versions", "Blocks",
        "CreationTime", "ModificationTime");
    assertLacksKeys(root,
        "volume", "bucket", "key", "completePath", "dataSize", "versions", "blockIds",
        "creationTime", "modificationTime");
  }

  /**
   * Same {@link KeyMetadata} instance: a default {@link ObjectMapper} (Java time only)
   * emits JavaBean names. That proves the naming split; Recon must not regress to that
   * mapping for API responses.
   */
  @Test
  public void keyMetadataNaiveJacksonDivergesFromJerseyWireFormat() throws Exception {
    KeyMetadata km = new KeyMetadata();
    km.setVolume("vol-a");
    km.setBucket("bucket-a");
    km.setKey("key-a");
    km.setCompletePath("/path");
    km.setDataSize(1L);
    km.setCreationTime(Instant.parse("2020-01-01T00:00:00Z"));
    km.setModificationTime(Instant.parse("2020-01-02T00:00:00Z"));

    ObjectMapper naive = new ObjectMapper().registerModule(new JavaTimeModule());
    JsonNode naiveNode = parse(naive.writeValueAsString(km));

    assertHasKeys(naiveNode, "volume", "bucket", "key");
    assertLacksKeys(naiveNode, "Volume", "Bucket", "Key");

    JsonNode jerseyNode = parse(toWireJson(km));
    assertHasKeys(jerseyNode, "Volume", "Bucket", "Key");
    assertLacksKeys(jerseyNode, "volume", "bucket", "key");
  }

  /**
   * {@link DatanodeMetadata} operational state must serialize as {@code opState}
   * (JAXB), not {@code operationalState} (derived from {@code getOperationalState()}).
   */
  @Test
  public void datanodeMetadataJerseyJsonUsesOpStateJaxbName() throws Exception {
    DatanodeMetadata dm = sampleDatanodeMetadataWithOpState();
    JsonNode root = parse(toWireJson(dm));
    assertEquals("IN_SERVICE", root.get("opState").asText());
    assertHasKeys(root,
        "uuid", "hostname", "state", "opState", "networkLocation", "version", "revision",
        "layoutVersion", "setupTime", "containers", "openContainers", "leaderCount",
        "pipelines");
    assertLacksKeys(root, "operationalState");
  }

  /**
   * Control: plain Jackson names the property {@code operationalState}. Recon's Jersey
   * stack must not produce that key for this type.
   */
  @Test
  public void datanodeMetadataNaiveJacksonUsesOperationalStatePropertyName()
      throws Exception {
    DatanodeMetadata dm = sampleDatanodeMetadataWithOpState();
    ObjectMapper naive = new ObjectMapper().registerModule(new JavaTimeModule());
    JsonNode naiveNode = parse(naive.writeValueAsString(dm));

    assertEquals("IN_SERVICE", naiveNode.get("operationalState").asText());
    assertHasKeys(naiveNode, "operationalState");
    assertLacksKeys(naiveNode, "opState");

    JsonNode jerseyNode = parse(toWireJson(dm));
    assertHasKeys(jerseyNode, "opState");
    assertLacksKeys(jerseyNode, "operationalState");
  }

  @Test
  public void unhealthyContainerMetadataJerseyJsonShape() throws Exception {
    JsonNode root = parse(toWireJson(sampleUnhealthyContainerMetadata()));
    assertHasKeys(root,
        "containerID", "containerState", "unhealthySince", "expectedReplicaCount",
        "actualReplicaCount", "replicaDeltaCount", "reason", "keys", "pipelineID",
        "replicas");
    assertEquals(100L, root.get("containerID").asLong());
    assertEquals("UNDER_REPLICATED", root.get("containerState").asText());
    assertEquals(42L, root.get("keys").asLong());
    assertTrue(root.get("replicas").isArray());
  }

  @Test
  public void unhealthyContainersResponseNestedMetadataKeepsJaxbKeys()
      throws Exception {
    UnhealthyContainersResponse body = new UnhealthyContainersResponse(
        Collections.singletonList(sampleUnhealthyContainerMetadata()));
    JsonNode root = parse(toWireJson(body));
    assertHasKeys(root,
        "missingCount", "underReplicatedCount", "overReplicatedCount",
        "misReplicatedCount", "replicaMismatchCount", "firstKey", "lastKey",
        "containers");
    JsonNode c0 = root.get("containers").get(0);
    assertHasKeys(c0,
        "containerID", "containerState", "pipelineID", "keys", "replicas");
  }

  @Test
  public void missingContainerMetadataJerseyJsonShape() throws Exception {
    JsonNode root = parse(toWireJson(sampleMissingContainerMetadata()));
    assertHasKeys(root,
        "containerID", "missingSince", "keys", "pipelineID", "replicas");
    assertEquals(200L, root.get("containerID").asLong());
    assertEquals(7L, root.get("keys").asLong());
    assertTrue(root.get("replicas").isArray());
  }

  @Test
  public void missingContainersResponseNestedMetadataKeepsJaxbKeys()
      throws Exception {
    MissingContainersResponse body = new MissingContainersResponse(1L,
        Collections.singletonList(sampleMissingContainerMetadata()));
    JsonNode root = parse(toWireJson(body));
    assertEquals(1L, root.get("totalCount").asLong());
    assertTrue(root.get("containers").isArray());
    JsonNode c0 = root.get("containers").get(0);
    assertHasKeys(c0, "containerID", "missingSince", "keys", "pipelineID",
        "replicas");
  }

  @Test
  public void removeDataNodesResponseWrapperJerseyJsonShape() throws Exception {
    RemoveDataNodesResponseWrapper body = new RemoveDataNodesResponseWrapper();
    Map<String, DatanodesResponse> map = new HashMap<>();
    DatanodeMetadata dn = DatanodeMetadata.newBuilder()
        .setHostname("wrapped.example")
        .setUuid("wrap-uuid")
        .setState(NodeState.HEALTHY)
        .setLeaderCount(1)
        .setContainers(1)
        .setOpenContainers(1)
        .setPipelines(Collections.emptyList())
        .build();
    map.put("rack-a", new DatanodesResponse(Collections.singletonList(dn)));
    body.setDatanodesResponseMap(map);

    JsonNode root = parse(toWireJson(body));
    assertHasKeys(root, "datanodesResponseMap");
    assertTrue(root.get("datanodesResponseMap").has("rack-a"));
    JsonNode inner = root.get("datanodesResponseMap").get("rack-a");
    assertEquals(1L, inner.get("totalCount").asLong());
    assertTrue(inner.get("datanodes").isArray());
    assertEquals(1, inner.get("datanodes").size());
  }

  /**
   * {@link ContainerMetadata} uses JAXB {@code ContainerID} / {@code NumberOfKeys}
   * (not lower-camel {@code containerID} / {@code numberOfKeys}).
   */
  @Test
  public void containerMetadataJerseyUsesJaxbElementNames() throws Exception {
    ContainerMetadata cm = sampleContainerMetadata();

    JsonNode root = parse(toWireJson(cm));
    assertEquals(99L, root.get("ContainerID").asLong());
    assertEquals(11L, root.get("NumberOfKeys").asLong());
    assertTrue(root.get("pipelines").isArray());
    assertLacksKeys(root, "containerID", "numberOfKeys");
  }

  @Test
  public void containerMetadataNaiveJacksonUsesBeanPropertyNames() throws Exception {
    ContainerMetadata cm = sampleContainerMetadata();

    ObjectMapper naive = new ObjectMapper().registerModule(new JavaTimeModule());
    JsonNode naiveNode = parse(naive.writeValueAsString(cm));
    assertEquals(99L, naiveNode.get("containerID").asLong());
    assertEquals(11L, naiveNode.get("numberOfKeys").asLong());
    assertLacksKeys(naiveNode, "ContainerID", "NumberOfKeys");

    JsonNode jerseyNode = parse(toWireJson(cm));
    assertHasKeys(jerseyNode, "ContainerID", "NumberOfKeys", "pipelines");
    assertLacksKeys(jerseyNode, "containerID", "numberOfKeys");
  }
}
