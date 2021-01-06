/*
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageSize;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE_DEFAULT;
import static org.junit.Assert.assertEquals;

/**
 * Unit test for Basic*OzoneFileSystem.
 */
@RunWith(Parameterized.class)
public class TestBasicOzoneFileSystems {

  private final FileSystem subject;

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(
        new Object[]{new BasicOzoneFileSystem()},
        new Object[]{new BasicRootedOzoneFileSystem()}
    );
  }

  public TestBasicOzoneFileSystems(FileSystem subject) {
    this.subject = subject;
  }

  @Test
  public void defaultBlockSize() {
    Configuration conf = new OzoneConfiguration();
    subject.setConf(conf);

    long expected = toBytes(OZONE_SCM_BLOCK_SIZE_DEFAULT);
    assertDefaultBlockSize(expected);
  }

  @Test
  public void defaultBlockSizeCustomized() {
    String customValue = "128MB";
    Configuration conf = new OzoneConfiguration();
    conf.set(OZONE_SCM_BLOCK_SIZE, customValue);
    subject.setConf(conf);

    assertDefaultBlockSize(toBytes(customValue));
  }

  private void assertDefaultBlockSize(long expected) {
    assertEquals(expected, subject.getDefaultBlockSize());

    Path anyPath = new Path("/");
    assertEquals(expected, subject.getDefaultBlockSize(anyPath));

    Path nonExistentFile = new Path("/no/such/file");
    assertEquals(expected, subject.getDefaultBlockSize(nonExistentFile));
  }

  private static long toBytes(String value) {
    StorageSize blockSize = StorageSize.parse(value);
    return (long) blockSize.getUnit().toBytes(blockSize.getValue());
  }
}
