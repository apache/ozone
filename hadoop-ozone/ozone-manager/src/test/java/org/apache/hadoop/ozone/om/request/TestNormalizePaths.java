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

import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.apache.hadoop.ozone.om.request.OMClientRequest.validateAndNormalizeKey;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Class to test normalize paths.
 */
public class TestNormalizePaths {
  @Test
  public void testNormalizePathsEnabled() throws Exception {

    Assertions.assertEquals("a/b/c/d",
        validateAndNormalizeKey(true, "a/b/c/d"));
    Assertions.assertEquals("a/b/c/d",
        validateAndNormalizeKey(true, "/a/b/c/d"));
    Assertions.assertEquals("a/b/c/d",
        validateAndNormalizeKey(true, "////a/b/c/d"));
    Assertions.assertEquals("a/b/c/d",
        validateAndNormalizeKey(true, "////a/b/////c/d"));
    Assertions.assertEquals("a/b/c/...../d",
        validateAndNormalizeKey(true, "////a/b/////c/...../d"));
    Assertions.assertEquals("a/b/d",
        validateAndNormalizeKey(true, "/a/b/c/../d"));
    Assertions.assertEquals("a",
        validateAndNormalizeKey(true, "a"));
    Assertions.assertEquals("a/b",
        validateAndNormalizeKey(true, "/a/./b"));
    Assertions.assertEquals("a/b",
        validateAndNormalizeKey(true, ".//a/./b"));
    Assertions.assertEquals("a/",
        validateAndNormalizeKey(true, "/a/."));
    Assertions.assertEquals("b/c",
        validateAndNormalizeKey(true, "//./b/c/"));
    Assertions.assertEquals("a/b/c/d",
        validateAndNormalizeKey(true, "a/b/c/d/"));
    Assertions.assertEquals("a/b/c/...../d",
        validateAndNormalizeKey(true, "////a/b/////c/...../d/"));
  }

  @Test
  public void testNormalizeKeyInvalidPaths() throws OMException {
    checkInvalidPath("/a/b/c/../../../../../d");
    checkInvalidPath("../a/b/c/");
    checkInvalidPath("/../..a/b/c/");
    checkInvalidPath("//");
    checkInvalidPath("/////");
    checkInvalidPath("");
    checkInvalidPath("/");
    checkInvalidPath("/:/:");
  }

  private void checkInvalidPath(String keyName) {
    try {
      validateAndNormalizeKey(true, keyName);
      fail("checkInvalidPath failed for path " + keyName);
    } catch (OMException ex) {
      Assertions.assertTrue(ex.getMessage().contains("Invalid KeyPath"));
    }
  }



  @Test
  public void testNormalizePathsDisable() throws OMException {

    Assertions.assertEquals("/a/b/c/d",
        validateAndNormalizeKey(false, "/a/b/c/d"));
    Assertions.assertEquals("////a/b/c/d",
        validateAndNormalizeKey(false, "////a/b/c/d"));
    Assertions.assertEquals("////a/b/////c/d",
        validateAndNormalizeKey(false, "////a/b/////c/d"));
    Assertions.assertEquals("////a/b/////c/...../d",
        validateAndNormalizeKey(false, "////a/b/////c/...../d"));
    Assertions.assertEquals("/a/b/c/../d",
        validateAndNormalizeKey(false, "/a/b/c/../d"));
    Assertions.assertEquals("/a/b/c/../../d",
        validateAndNormalizeKey(false, "/a/b/c/../../d"));
  }
}
