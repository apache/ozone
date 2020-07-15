/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.ozone.om.request;

import org.junit.Assert;
import org.junit.Test;

import static org.apache.hadoop.ozone.om.request.OMClientRequest.getNormalizedKey;

/**
 * Class to test normalize paths.
 */
public class TestNormalizePaths {

  @Test
  public void testNormalizePathsEnabled() {

    Assert.assertEquals("a/b/c/d",
        getNormalizedKey(true, "a/b/c/d"));
    Assert.assertEquals("a/b/c/d",
        getNormalizedKey(true, "/a/b/c/d"));
    Assert.assertEquals("a/b/c/d",
        getNormalizedKey(true, "////a/b/c/d"));
    Assert.assertEquals("a/b/c/d",
        getNormalizedKey(true, "////a/b/////c/d"));
    Assert.assertEquals("a/b/c/...../d",
        getNormalizedKey(true, "////a/b/////c/...../d"));
    Assert.assertEquals("a/b/d",
        getNormalizedKey(true, "/a/b/c/../d"));
    Assert.assertEquals("../../d",
        getNormalizedKey(true, "/a/b/c/../../../../../d"));
    Assert.assertEquals("../a/b/c",
        getNormalizedKey(true, "../a/b/c/"));
    Assert.assertEquals("../a/b/c",
        getNormalizedKey(true, "/../a/b/c/"));
    Assert.assertEquals("a",
        getNormalizedKey(true, "a"));
    Assert.assertEquals("",
        getNormalizedKey(true, ""));
    Assert.assertEquals("",
        getNormalizedKey(true, "/"));
  }



  @Test
  public void testNormalizePathsDisable() {

    Assert.assertEquals("/a/b/c/d",
        getNormalizedKey(false, "/a/b/c/d"));
    Assert.assertEquals("////a/b/c/d",
        getNormalizedKey(false, "////a/b/c/d"));
    Assert.assertEquals("////a/b/////c/d",
        getNormalizedKey(false, "////a/b/////c/d"));
    Assert.assertEquals("////a/b/////c/...../d",
        getNormalizedKey(false, "////a/b/////c/...../d"));
    Assert.assertEquals("/a/b/c/../d",
        getNormalizedKey(false, "/a/b/c/../d"));
    Assert.assertEquals("/a/b/c/../../d",
        getNormalizedKey(false, "/a/b/c/../../d"));
  }
}
