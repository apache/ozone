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
package org.apache.hadoop.fs.ozone;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.net.URI;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Rooted Ozone File system tests that are light weight and use mocks.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ OzoneClientFactory.class, UserGroupInformation.class })
@PowerMockIgnore("javax.management.*")
public class TestRootedOzoneFileSystemWithMocks {

  // Mock tests should finish in seconds.
  @Rule
  public Timeout timeout = new Timeout(45_000);

  @Test
  public void testFSUriWithHostPortOverrides() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    OzoneClient ozoneClient = mock(OzoneClient.class);
    ObjectStore objectStore = mock(ObjectStore.class);

    when(ozoneClient.getObjectStore()).thenReturn(objectStore);

    PowerMockito.mockStatic(OzoneClientFactory.class);
    PowerMockito.when(OzoneClientFactory.getRpcClient(eq("local.host"),
        eq(5899), eq(conf))).thenReturn(ozoneClient);

    UserGroupInformation ugi = mock(UserGroupInformation.class);
    PowerMockito.mockStatic(UserGroupInformation.class);
    PowerMockito.when(UserGroupInformation.getCurrentUser()).thenReturn(ugi);
    when(ugi.getShortUserName()).thenReturn("user1");

    // Note: FileSystem#loadFileSystems doesn't load OFS class because
    //  META-INF still points to org.apache.hadoop.fs.ozone.OzoneFileSystem
    conf.set("fs.ofs.impl", "org.apache.hadoop.fs.ozone.RootedOzoneFileSystem");

    URI uri = new URI("ofs://local.host:5899");
    FileSystem fileSystem = FileSystem.get(uri, conf);

    assertEquals(fileSystem.getUri().getAuthority(), "local.host:5899");
    PowerMockito.verifyStatic();
    OzoneClientFactory.getRpcClient("local.host", 5899, conf);
  }

  @Test
  public void testFSUriWithHostPortUnspecified() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    final int omPort = OmUtils.getOmRpcPort(conf);

    OzoneClient ozoneClient = mock(OzoneClient.class);
    ObjectStore objectStore = mock(ObjectStore.class);

    when(ozoneClient.getObjectStore()).thenReturn(objectStore);

    PowerMockito.mockStatic(OzoneClientFactory.class);
    PowerMockito.when(OzoneClientFactory.getRpcClient(eq("local.host"),
        eq(omPort), eq(conf))).thenReturn(ozoneClient);

    UserGroupInformation ugi = mock(UserGroupInformation.class);
    PowerMockito.mockStatic(UserGroupInformation.class);
    PowerMockito.when(UserGroupInformation.getCurrentUser()).thenReturn(ugi);
    when(ugi.getShortUserName()).thenReturn("user1");

    // Note: FileSystem#loadFileSystems doesn't load OFS class because
    //  META-INF still points to org.apache.hadoop.fs.ozone.OzoneFileSystem
    conf.set("fs.ofs.impl", "org.apache.hadoop.fs.ozone.RootedOzoneFileSystem");

    URI uri = new URI("ofs://local.host");
    FileSystem fileSystem = FileSystem.get(uri, conf);

    assertEquals(fileSystem.getUri().getHost(), "local.host");
    // The URI doesn't contain a port number, expect -1 from getPort()
    assertEquals(fileSystem.getUri().getPort(), -1);
    PowerMockito.verifyStatic();
    // Check the actual port number in use
    OzoneClientFactory.getRpcClient("local.host", omPort, conf);
  }
}
