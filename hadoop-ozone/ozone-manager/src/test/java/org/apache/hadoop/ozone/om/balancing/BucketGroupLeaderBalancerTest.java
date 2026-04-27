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

package org.apache.hadoop.ozone.om.balancing;

import static com.google.common.collect.Lists.newArrayList;
import static java.util.UUID.nameUUIDFromBytes;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class BucketGroupLeaderBalancerTest {

  private final ArrayList<RaftPeerId> raftPeerIds = newArrayList(
          RaftPeerId.valueOf("om1"),
          RaftPeerId.valueOf("om2"),
          RaftPeerId.valueOf("om3")
  );

  @ParameterizedTest
  @MethodSource("alreadyBalancedGroups")
  public void testGetEmptyResponseWhenGroupsAlreadyBalanced(Map<RaftGroupId, RaftPeerId> groupsWithLeaderInfo) {
    List<LeaderChangingGroupInfo> leaderChangingInfo =
            BucketGroupLeaderBalancer.getLeaderChangingInfo(groupsWithLeaderInfo, new HashSet<>(raftPeerIds));
    assertEquals(0, leaderChangingInfo.size());
  }

  private static Stream<Map<RaftGroupId, RaftPeerId>> alreadyBalancedGroups() {
    Map<RaftGroupId, RaftPeerId> groupsWithLeaderInfo = new HashMap<>();
    groupsWithLeaderInfo.put(RaftGroupId.randomId(), RaftPeerId.valueOf("om1"));
    groupsWithLeaderInfo.put(RaftGroupId.randomId(), RaftPeerId.valueOf("om2"));
    groupsWithLeaderInfo.put(RaftGroupId.randomId(), RaftPeerId.valueOf("om3"));
    groupsWithLeaderInfo.put(RaftGroupId.randomId(), RaftPeerId.valueOf("om1"));
    groupsWithLeaderInfo.put(RaftGroupId.randomId(), RaftPeerId.valueOf("om2"));
    groupsWithLeaderInfo.put(RaftGroupId.randomId(), RaftPeerId.valueOf("om3"));

    Map<RaftGroupId, RaftPeerId> groupsWithLeaderInfo1 = new HashMap<>();
    groupsWithLeaderInfo1.put(RaftGroupId.randomId(), RaftPeerId.valueOf("om1"));
    groupsWithLeaderInfo1.put(RaftGroupId.randomId(), RaftPeerId.valueOf("om2"));
    groupsWithLeaderInfo1.put(RaftGroupId.randomId(), RaftPeerId.valueOf("om3"));
    groupsWithLeaderInfo1.put(RaftGroupId.randomId(), RaftPeerId.valueOf("om1"));
    groupsWithLeaderInfo1.put(RaftGroupId.randomId(), RaftPeerId.valueOf("om2"));

    Map<RaftGroupId, RaftPeerId> groupsWithLeaderInfo2 = new HashMap<>();
    groupsWithLeaderInfo2.put(RaftGroupId.randomId(), RaftPeerId.valueOf("om1"));
    groupsWithLeaderInfo2.put(RaftGroupId.randomId(), RaftPeerId.valueOf("om2"));
    groupsWithLeaderInfo2.put(RaftGroupId.randomId(), RaftPeerId.valueOf("om3"));
    groupsWithLeaderInfo2.put(RaftGroupId.randomId(), RaftPeerId.valueOf("om1"));

    Map<RaftGroupId, RaftPeerId> groupsWithLeaderInfo3 = new HashMap<>();
    groupsWithLeaderInfo3.put(RaftGroupId.randomId(), RaftPeerId.valueOf("om1"));
    groupsWithLeaderInfo3.put(RaftGroupId.randomId(), RaftPeerId.valueOf("om2"));
    groupsWithLeaderInfo3.put(RaftGroupId.randomId(), RaftPeerId.valueOf("om3"));
    groupsWithLeaderInfo3.put(RaftGroupId.randomId(), RaftPeerId.valueOf("om1"));

    Map<RaftGroupId, RaftPeerId> groupsWithLeaderInfo4 = new HashMap<>();
    groupsWithLeaderInfo4.put(RaftGroupId.randomId(), RaftPeerId.valueOf("om1"));
    groupsWithLeaderInfo4.put(RaftGroupId.randomId(), RaftPeerId.valueOf("om2"));
    groupsWithLeaderInfo4.put(RaftGroupId.randomId(), RaftPeerId.valueOf("om3"));

    Map<RaftGroupId, RaftPeerId> groupsWithLeaderInfo5 = new HashMap<>();
    groupsWithLeaderInfo5.put(RaftGroupId.randomId(), RaftPeerId.valueOf("om1"));
    groupsWithLeaderInfo5.put(RaftGroupId.randomId(), RaftPeerId.valueOf("om2"));

    Map<RaftGroupId, RaftPeerId> groupsWithLeaderInfo6 = new HashMap<>();
    groupsWithLeaderInfo6.put(RaftGroupId.randomId(), RaftPeerId.valueOf("om1"));

    return Stream.of(
            groupsWithLeaderInfo,
            groupsWithLeaderInfo1,
            groupsWithLeaderInfo2,
            groupsWithLeaderInfo3,
            groupsWithLeaderInfo4,
            groupsWithLeaderInfo5,
            groupsWithLeaderInfo6
    );
  }

  @ParameterizedTest
  @MethodSource("unBalancedGroups")
  public void testBalancingGroupWhenOneOmIsLeaderForAll(Map<RaftGroupId, RaftPeerId> groupsWithLeaderInfo) {
    List<LeaderChangingGroupInfo> leaderChangingInfo =
            BucketGroupLeaderBalancer.getLeaderChangingInfo(groupsWithLeaderInfo, new HashSet<>(raftPeerIds));
    assertGroupingState(leaderChangingInfo, groupsWithLeaderInfo, newArrayList(2L, 1L, 1L));
  }

  private static Stream<Map<RaftGroupId, RaftPeerId>> unBalancedGroups() {
    Map<RaftGroupId, RaftPeerId> groupsWithLeaderInfo = new HashMap<>();
    groupsWithLeaderInfo.put(RaftGroupId.randomId(), RaftPeerId.valueOf("om1"));
    groupsWithLeaderInfo.put(RaftGroupId.randomId(), RaftPeerId.valueOf("om1"));
    groupsWithLeaderInfo.put(RaftGroupId.randomId(), RaftPeerId.valueOf("om1"));
    groupsWithLeaderInfo.put(RaftGroupId.randomId(), RaftPeerId.valueOf("om1"));

    Map<RaftGroupId, RaftPeerId> groupsWithLeaderInfo1 = new HashMap<>();
    groupsWithLeaderInfo1.put(RaftGroupId.randomId(), RaftPeerId.valueOf("om2"));
    groupsWithLeaderInfo1.put(RaftGroupId.randomId(), RaftPeerId.valueOf("om2"));
    groupsWithLeaderInfo1.put(RaftGroupId.randomId(), RaftPeerId.valueOf("om2"));
    groupsWithLeaderInfo1.put(RaftGroupId.randomId(), RaftPeerId.valueOf("om2"));

    Map<RaftGroupId, RaftPeerId> groupsWithLeaderInfo2 = new HashMap<>();
    groupsWithLeaderInfo2.put(RaftGroupId.randomId(), RaftPeerId.valueOf("om3"));
    groupsWithLeaderInfo2.put(RaftGroupId.randomId(), RaftPeerId.valueOf("om3"));
    groupsWithLeaderInfo2.put(RaftGroupId.randomId(), RaftPeerId.valueOf("om3"));
    groupsWithLeaderInfo2.put(RaftGroupId.randomId(), RaftPeerId.valueOf("om3"));

    return Stream.of(
            groupsWithLeaderInfo,
            groupsWithLeaderInfo1,
            groupsWithLeaderInfo2
    );
  }

  @ParameterizedTest
  @MethodSource("equalUnBalancedGroups")
  public void testEqualBalancing(Map<RaftGroupId, RaftPeerId> groupsWithLeaderInfo) {
    List<LeaderChangingGroupInfo> leaderChangingInfo =
            BucketGroupLeaderBalancer.getLeaderChangingInfo(groupsWithLeaderInfo, new HashSet<>(raftPeerIds));

    assertGroupingState(leaderChangingInfo, groupsWithLeaderInfo, newArrayList(2L, 2L, 2L));
  }

  @Test
  public void testAscendingOrderBalancing() {

    Map<RaftGroupId, RaftPeerId> groupsWithLeaderInfo = new HashMap<>();
    // 13
    addGroupToPeerIdInfoToMap("07152D234B70", "om1", groupsWithLeaderInfo);
    addGroupToPeerIdInfoToMap("2254211691B5", "om1", groupsWithLeaderInfo);
    addGroupToPeerIdInfoToMap("133F073095DD", "om1", groupsWithLeaderInfo);
    addGroupToPeerIdInfoToMap("F38708677C48", "om1", groupsWithLeaderInfo);
    addGroupToPeerIdInfoToMap("862EA77393DD", "om1", groupsWithLeaderInfo);
    addGroupToPeerIdInfoToMap("8FD4C9EC6997", "om1", groupsWithLeaderInfo);
    addGroupToPeerIdInfoToMap("32B957B6F332", "om1", groupsWithLeaderInfo);
    addGroupToPeerIdInfoToMap("27599FA62642", "om1", groupsWithLeaderInfo);
    addGroupToPeerIdInfoToMap("A6CE92BF5ADD", "om1", groupsWithLeaderInfo);
    addGroupToPeerIdInfoToMap("499D7B701668", "om1", groupsWithLeaderInfo);
    addGroupToPeerIdInfoToMap("D78622277C4E", "om1", groupsWithLeaderInfo);
    addGroupToPeerIdInfoToMap("36D0ED79BC43", "om1", groupsWithLeaderInfo);
    addGroupToPeerIdInfoToMap("0B968DBBBD68", "om1", groupsWithLeaderInfo);

    // 13
    addGroupToPeerIdInfoToMap("2E7E7BACC9F2", "om2", groupsWithLeaderInfo);
    addGroupToPeerIdInfoToMap("60CC007E8B9D", "om2", groupsWithLeaderInfo);
    addGroupToPeerIdInfoToMap("3D89A146B445", "om2", groupsWithLeaderInfo);
    addGroupToPeerIdInfoToMap("68001E7E0E54", "om2", groupsWithLeaderInfo);
    addGroupToPeerIdInfoToMap("8A9BF8AE7D0B", "om2", groupsWithLeaderInfo);
    addGroupToPeerIdInfoToMap("48C591B8F430", "om2", groupsWithLeaderInfo);
    addGroupToPeerIdInfoToMap("A08AF0923DE0", "om2", groupsWithLeaderInfo);
    addGroupToPeerIdInfoToMap("1415E770C6DD", "om2", groupsWithLeaderInfo);
    addGroupToPeerIdInfoToMap("2A47751A186F", "om2", groupsWithLeaderInfo);
    addGroupToPeerIdInfoToMap("779273E95AA6", "om2", groupsWithLeaderInfo);
    addGroupToPeerIdInfoToMap("D152ADD07294", "om2", groupsWithLeaderInfo);
    addGroupToPeerIdInfoToMap("A7D494132EE5", "om2", groupsWithLeaderInfo);
    addGroupToPeerIdInfoToMap("DFF913DF997A", "om2", groupsWithLeaderInfo);

    // 14
    addGroupToPeerIdInfoToMap("EACD77ACCE5E", "om3", groupsWithLeaderInfo);
    addGroupToPeerIdInfoToMap("C76A704E9B52", "om3", groupsWithLeaderInfo);
    addGroupToPeerIdInfoToMap("96A5EE53A70F", "om3", groupsWithLeaderInfo);
    addGroupToPeerIdInfoToMap("14EC9237B6EC", "om3", groupsWithLeaderInfo);
    addGroupToPeerIdInfoToMap("E1F8CBE183B9", "om3", groupsWithLeaderInfo);
    addGroupToPeerIdInfoToMap("B9FED0FF9548", "om3", groupsWithLeaderInfo);
    addGroupToPeerIdInfoToMap("477183B7AC3F", "om3", groupsWithLeaderInfo);
    addGroupToPeerIdInfoToMap("E1C2E8A762BD", "om3", groupsWithLeaderInfo);
    addGroupToPeerIdInfoToMap("879C71B5DB5C", "om3", groupsWithLeaderInfo);
    addGroupToPeerIdInfoToMap("3E5C3CD9CA25", "om3", groupsWithLeaderInfo);
    addGroupToPeerIdInfoToMap("19F2D8D9524D", "om3", groupsWithLeaderInfo);
    addGroupToPeerIdInfoToMap("C3003946C48F", "om3", groupsWithLeaderInfo);
    addGroupToPeerIdInfoToMap("FFDC3E463904", "om3", groupsWithLeaderInfo);
    addGroupToPeerIdInfoToMap("4469D2CF9F3E", "om3", groupsWithLeaderInfo);

    List<LeaderChangingGroupInfo> leaderChangingInfo =
            BucketGroupLeaderBalancer.getLeaderChangingInfo(groupsWithLeaderInfo, new HashSet<>(raftPeerIds));

    assertGroupingState(leaderChangingInfo, groupsWithLeaderInfo, newArrayList(14L, 13L, 13L));
  }

  private void addGroupToPeerIdInfoToMap(
          String groupId,
          String nodeId,
          Map<RaftGroupId, RaftPeerId> groupsWithLeaderInfo
  ) {
    groupsWithLeaderInfo.put(
            RaftGroupId.valueOf(nameUUIDFromBytes(groupId.getBytes(StandardCharsets.UTF_8))),
            RaftPeerId.valueOf(nodeId)
    );
  }

  @ParameterizedTest
  @MethodSource("om1HasNotMaxNodesCount")
  public void testNodeCountsShouldBeDecreased(Map<RaftGroupId, RaftPeerId> groupsWithLeaderInfo) {
    boolean isBalanced =
            BucketGroupLeaderBalancer.isAlreadyBalanced(groupsWithLeaderInfo, raftPeerIds.size());
    assertFalse(isBalanced);
  }

  private static Stream<Map<RaftGroupId, RaftPeerId>> equalUnBalancedGroups() {
    Map<RaftGroupId, RaftPeerId> groupsWithLeaderInfo = new HashMap<>();
    groupsWithLeaderInfo.put(RaftGroupId.randomId(), RaftPeerId.valueOf("om1"));
    groupsWithLeaderInfo.put(RaftGroupId.randomId(), RaftPeerId.valueOf("om1"));
    groupsWithLeaderInfo.put(RaftGroupId.randomId(), RaftPeerId.valueOf("om1"));
    groupsWithLeaderInfo.put(RaftGroupId.randomId(), RaftPeerId.valueOf("om1"));
    groupsWithLeaderInfo.put(RaftGroupId.randomId(), RaftPeerId.valueOf("om1"));
    groupsWithLeaderInfo.put(RaftGroupId.randomId(), RaftPeerId.valueOf("om1"));

    Map<RaftGroupId, RaftPeerId> groupsWithLeaderInfo1 = new HashMap<>();
    groupsWithLeaderInfo1.put(RaftGroupId.randomId(), RaftPeerId.valueOf("om2"));
    groupsWithLeaderInfo1.put(RaftGroupId.randomId(), RaftPeerId.valueOf("om2"));
    groupsWithLeaderInfo1.put(RaftGroupId.randomId(), RaftPeerId.valueOf("om2"));
    groupsWithLeaderInfo1.put(RaftGroupId.randomId(), RaftPeerId.valueOf("om2"));
    groupsWithLeaderInfo1.put(RaftGroupId.randomId(), RaftPeerId.valueOf("om2"));
    groupsWithLeaderInfo1.put(RaftGroupId.randomId(), RaftPeerId.valueOf("om2"));

    Map<RaftGroupId, RaftPeerId> groupsWithLeaderInfo2 = new HashMap<>();
    groupsWithLeaderInfo2.put(RaftGroupId.randomId(), RaftPeerId.valueOf("om3"));
    groupsWithLeaderInfo2.put(RaftGroupId.randomId(), RaftPeerId.valueOf("om3"));
    groupsWithLeaderInfo2.put(RaftGroupId.randomId(), RaftPeerId.valueOf("om3"));
    groupsWithLeaderInfo2.put(RaftGroupId.randomId(), RaftPeerId.valueOf("om3"));
    groupsWithLeaderInfo2.put(RaftGroupId.randomId(), RaftPeerId.valueOf("om3"));
    groupsWithLeaderInfo2.put(RaftGroupId.randomId(), RaftPeerId.valueOf("om3"));

    return Stream.of(
            groupsWithLeaderInfo,
            groupsWithLeaderInfo1,
            groupsWithLeaderInfo2
    );
  }

  private static Stream<Map<RaftGroupId, RaftPeerId>> om1HasNotMaxNodesCount() {
    Map<RaftGroupId, RaftPeerId> groupsWithLeaderInfo = new HashMap<>();
    groupsWithLeaderInfo.put(RaftGroupId.randomId(), RaftPeerId.valueOf("om1"));
    groupsWithLeaderInfo.put(RaftGroupId.randomId(), RaftPeerId.valueOf("om2"));
    groupsWithLeaderInfo.put(RaftGroupId.randomId(), RaftPeerId.valueOf("om2"));
    groupsWithLeaderInfo.put(RaftGroupId.randomId(), RaftPeerId.valueOf("om3"));
    groupsWithLeaderInfo.put(RaftGroupId.randomId(), RaftPeerId.valueOf("om3"));

    Map<RaftGroupId, RaftPeerId> groupsWithLeaderInfo1 = new HashMap<>();
    groupsWithLeaderInfo1.put(RaftGroupId.randomId(), RaftPeerId.valueOf("om1"));
    groupsWithLeaderInfo1.put(RaftGroupId.randomId(), RaftPeerId.valueOf("om2"));
    groupsWithLeaderInfo1.put(RaftGroupId.randomId(), RaftPeerId.valueOf("om2"));
    groupsWithLeaderInfo1.put(RaftGroupId.randomId(), RaftPeerId.valueOf("om3"));

    Map<RaftGroupId, RaftPeerId> groupsWithLeaderInfo2 = new HashMap<>();
    groupsWithLeaderInfo2.put(RaftGroupId.randomId(), RaftPeerId.valueOf("om1"));
    groupsWithLeaderInfo2.put(RaftGroupId.randomId(), RaftPeerId.valueOf("om2"));
    groupsWithLeaderInfo2.put(RaftGroupId.randomId(), RaftPeerId.valueOf("om3"));
    groupsWithLeaderInfo2.put(RaftGroupId.randomId(), RaftPeerId.valueOf("om3"));
    return Stream.of(
            groupsWithLeaderInfo,
            groupsWithLeaderInfo1,
            groupsWithLeaderInfo2
    );
  }

  private static void assertGroupingState(
          List<LeaderChangingGroupInfo> leaderChangingInfo,
          Map<RaftGroupId, RaftPeerId> groupsWithLeaderInfo,
          List<Long> variablesToCheck
  ) {
    for (LeaderChangingGroupInfo leaderChangingGroupInfo : leaderChangingInfo) {
      groupsWithLeaderInfo.put(leaderChangingGroupInfo.getGroupId(), leaderChangingGroupInfo.getNewLeader());
    }
    Map<String, Long> groupedStateMap = groupsWithLeaderInfo.entrySet().stream()
            .collect(
                    Collectors.groupingBy(
                            it -> it.getValue().toString(), Collectors.counting()
                    )
            );
    for (int i = 1; i <= groupedStateMap.size(); i++) {
      assertEquals(variablesToCheck.get(i - 1), groupedStateMap.get("om" + i));
    }
  }
}
