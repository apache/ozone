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

package org.apache.hadoop.ozone.shell.acl;

import static org.apache.hadoop.ozone.OzoneAcl.AclScope.ACCESS;
import static org.apache.hadoop.ozone.OzoneAcl.AclScope.DEFAULT;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLIdentityType.GROUP;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLIdentityType.USER;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.ALL;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.READ;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.WRITE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
import org.apache.hadoop.ozone.shell.bucket.GetAclBucketHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

/**
 * Tests for GetAclHandler with string format output.
 */
public class TestGetAclHandler {

  private TestableGetAclBucketHandler cmd;
  private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
  private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
  private final PrintStream originalOut = System.out;
  private final PrintStream originalErr = System.err;
  private static final String DEFAULT_ENCODING = StandardCharsets.UTF_8.name();

  /**
   * Testable version of GetAclBucketHandler that exposes execute method.
   */
  static class TestableGetAclBucketHandler extends GetAclBucketHandler {
    public void publicExecute(OzoneClient client, OzoneObj obj)
        throws IOException {
      execute(client, obj);
    }
  }

  @BeforeEach
  public void setup() throws UnsupportedEncodingException {
    cmd = new TestableGetAclBucketHandler();
    System.setOut(new PrintStream(outContent, false, DEFAULT_ENCODING));
    System.setErr(new PrintStream(errContent, false, DEFAULT_ENCODING));
  }

  @AfterEach
  public void tearDown() {
    System.setOut(originalOut);
    System.setErr(originalErr);
  }

  @Test
  public void testGetAclAsJson() throws IOException {
    // Setup mock objects
    OzoneClient client = mock(OzoneClient.class);
    ObjectStore objectStore = mock(ObjectStore.class);
    when(client.getObjectStore()).thenReturn(objectStore);

    List<OzoneAcl> acls = Arrays.asList(
        OzoneAcl.of(USER, "user1", ACCESS, READ, WRITE),
        OzoneAcl.of(GROUP, "hadoop", ACCESS, ALL)
    );

    OzoneObj obj = OzoneObjInfo.Builder.newBuilder()
        .setBucketName("bucket")
        .setVolumeName("volume")
        .setResType(OzoneObj.ResourceType.BUCKET)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .build();

    when(objectStore.getAcl(obj)).thenReturn(acls);

    // Parse command line without flag (default JSON output)
    CommandLine commandLine = new CommandLine(cmd);
    commandLine.parseArgs("o3://ozone1/volume/bucket");

    // Execute command with default JSON output
    cmd.publicExecute(client, obj);

    // Verify JSON output
    ObjectMapper mapper = new ObjectMapper();
    JsonNode json = mapper.readTree(outContent.toString(DEFAULT_ENCODING));
    assertTrue(json.isArray());
    assertEquals(2, json.size());
    assertEquals("USER", json.get(0).get("type").asText());
    assertEquals("user1", json.get(0).get("name").asText());
    assertEquals("GROUP", json.get(1).get("type").asText());
    assertEquals("hadoop", json.get(1).get("name").asText());
  }

  @Test
  public void testGetAclAsStringWithAccessScope() throws IOException {
    // Setup mock objects
    OzoneClient client = mock(OzoneClient.class);
    ObjectStore objectStore = mock(ObjectStore.class);
    when(client.getObjectStore()).thenReturn(objectStore);

    List<OzoneAcl> acls = Arrays.asList(
        OzoneAcl.of(USER, "user1", ACCESS, READ, WRITE),
        OzoneAcl.of(GROUP, "hadoop", ACCESS, ALL)
    );

    OzoneObj obj = OzoneObjInfo.Builder.newBuilder()
        .setBucketName("bucket")
        .setVolumeName("volume")
        .setResType(OzoneObj.ResourceType.BUCKET)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .build();

    when(objectStore.getAcl(obj)).thenReturn(acls);

    // Parse command line with --json=false flag (string output)
    CommandLine commandLine = new CommandLine(cmd);
    commandLine.parseArgs("--json=false", "o3://ozone1/volume/bucket");

    // Execute command with --json=false flag
    cmd.publicExecute(client, obj);

    // Verify string output (should omit [ACCESS] for default scope)
    String output = outContent.toString(DEFAULT_ENCODING).trim();
    assertEquals("user:user1:rw,group:hadoop:a", output);
  }

  @Test
  public void testGetAclAsStringWithDefaultScope() throws IOException {
    // Setup mock objects
    OzoneClient client = mock(OzoneClient.class);
    ObjectStore objectStore = mock(ObjectStore.class);
    when(client.getObjectStore()).thenReturn(objectStore);

    List<OzoneAcl> acls = Arrays.asList(
        OzoneAcl.of(USER, "user1", DEFAULT, READ, WRITE),
        OzoneAcl.of(GROUP, "hadoop", DEFAULT, ALL)
    );

    OzoneObj obj = OzoneObjInfo.Builder.newBuilder()
        .setBucketName("bucket")
        .setVolumeName("volume")
        .setResType(OzoneObj.ResourceType.BUCKET)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .build();

    when(objectStore.getAcl(obj)).thenReturn(acls);

    // Parse command line with --json=false flag (string output)
    CommandLine commandLine = new CommandLine(cmd);
    commandLine.parseArgs("--json=false", "o3://ozone1/volume/bucket");

    // Execute command with --json=false flag
    cmd.publicExecute(client, obj);

    // Verify string output (should include [DEFAULT] for non-default scope)
    String output = outContent.toString(DEFAULT_ENCODING).trim();
    assertEquals("user:user1:rw[DEFAULT],group:hadoop:a[DEFAULT]", output);
  }

  @Test
  public void testGetAclAsStringMixedScopes() throws IOException {
    // Setup mock objects
    OzoneClient client = mock(OzoneClient.class);
    ObjectStore objectStore = mock(ObjectStore.class);
    when(client.getObjectStore()).thenReturn(objectStore);

    List<OzoneAcl> acls = Arrays.asList(
        OzoneAcl.of(USER, "user1", ACCESS, READ, WRITE),
        OzoneAcl.of(GROUP, "hadoop", DEFAULT, ALL)
    );

    OzoneObj obj = OzoneObjInfo.Builder.newBuilder()
        .setBucketName("bucket")
        .setVolumeName("volume")
        .setResType(OzoneObj.ResourceType.BUCKET)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .build();

    when(objectStore.getAcl(obj)).thenReturn(acls);

    // Parse command line with --json=false flag (string output)
    CommandLine commandLine = new CommandLine(cmd);
    commandLine.parseArgs("--json=false", "o3://ozone1/volume/bucket");

    // Execute command with --json=false flag
    cmd.publicExecute(client, obj);

    // Verify string output (mixed scopes)
    String output = outContent.toString(DEFAULT_ENCODING).trim();
    assertEquals("user:user1:rw,group:hadoop:a[DEFAULT]", output);
  }

}
