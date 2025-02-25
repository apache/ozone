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

package org.apache.hadoop.hdds.scm;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_NODES_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_NODE_ID_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_SERVICE_IDS_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Collection;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ha.SCMHAUtils;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@code SCMHAUtils}.
 */
class TestSCMHAUtils {

  @Test
  void testRemoveSelfId() {
    String service = "mySCM";
    String selfId = "scm3";

    OzoneConfiguration input = new OzoneConfiguration();
    input.set(OZONE_SCM_SERVICE_IDS_KEY, service);
    input.set(OZONE_SCM_NODES_KEY + "." + service, "scm1,scm2," + selfId);
    input.set(OZONE_SCM_NODE_ID_KEY, selfId);

    OzoneConfiguration output = SCMHAUtils.removeSelfId(input, selfId);
    Collection<String> nodesWithoutSelf =
        HddsUtils.getSCMNodeIds(output, service);

    assertEquals(2, nodesWithoutSelf.size());
    assertThat(nodesWithoutSelf).doesNotContain(selfId);
  }
}
