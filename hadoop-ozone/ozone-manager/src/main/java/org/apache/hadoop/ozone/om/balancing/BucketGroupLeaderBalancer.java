package org.apache.hadoop.ozone.om.balancing;

import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Class for balancing leaders of multi raft bucket groups.
 */
public final class BucketGroupLeaderBalancer {

  private static final Logger LOG = LoggerFactory.getLogger(BucketGroupLeaderBalancer.class);

  private BucketGroupLeaderBalancer() {
  }

  public static List<LeaderChangingGroupInfo> getLeaderChangingInfo(Map<RaftGroupId, RaftPeerId> groupsWithLeaderInfo,
                                                                    Set<RaftPeerId> peerIdSet) {
    if (isAlreadyBalanced(groupsWithLeaderInfo, peerIdSet.size())) {
      return Collections.emptyList();
    }

    List<LeaderChangingGroupInfo> leaderChangingGroupInfoList = new ArrayList<>();
    int counter = 0;
    RaftPeerId[] peerIdSetArray = new RaftPeerId[peerIdSet.size()];
    peerIdSet.toArray(peerIdSetArray);
    Arrays.sort(peerIdSetArray, Comparator.comparing(RaftPeerId::toString));

    for (Map.Entry<RaftGroupId, RaftPeerId> entry : groupsWithLeaderInfo.entrySet()) {
      RaftPeerId newLeader = peerIdSetArray[counter % peerIdSetArray.length];
      if (!newLeader.equals(entry.getValue())) {
        leaderChangingGroupInfoList.add(
            new LeaderChangingGroupInfo(
                entry.getKey(),
                entry.getValue(),
                newLeader
            )
        );
      }
      counter++;
    }

    return leaderChangingGroupInfoList;
  }

  public static boolean isAlreadyBalanced(Map<RaftGroupId, RaftPeerId> groupsWithLeaderInfo, int totalPeers) {

    Map<RaftPeerId, Long> raftGroupLeadersCountMap = groupsWithLeaderInfo.entrySet().stream().collect(
        Collectors.groupingBy(
            Map.Entry::getValue, Collectors.counting()
        )
    );
    int raftGroupLeadersCountMapSize = raftGroupLeadersCountMap.size();
    if (totalPeers != raftGroupLeadersCountMapSize) {
      LOG.trace("Unbalanced group. Total peers: {}. Raft group leaders map size: {}",
              totalPeers,
              raftGroupLeadersCountMapSize);
      return false;
    }
    List<Long> distinctValues = raftGroupLeadersCountMap.values().stream().distinct().collect(Collectors.toList());
    Long maxValue = distinctValues.stream().max(Long::compareTo).orElse(0L);
    Long minValue = distinctValues.stream().min(Long::compareTo).orElse(0L);
    if (!isNodesCountDecrease(raftGroupLeadersCountMap)) {
      return false;
    }
    if (maxValue - minValue <= 1) {
      LOG.trace("Bucket groups is already balanced");
      return true;
    } else {
      LOG.trace("Bucket groups is not balanced. Max value: {}. Min value: {}.", maxValue, minValue);
      return false;
    }
  }

  /**
   * Om1 should has max number. Om2 should not be greater than Om1. Om3 should not be greater than Om2.
   * @param raftGroupLeadersCountMap  peers to groups count map
   * @return is nodes count decrease
   */
  private static boolean isNodesCountDecrease(Map<RaftPeerId, Long> raftGroupLeadersCountMap) {
    List<RaftPeerId> keys =
        raftGroupLeadersCountMap.keySet().stream()
                .sorted(Comparator.comparing(RaftPeerId::toString))
                .collect(Collectors.toList());
    Long prevVal = null;
    for (RaftPeerId raftPeerId : keys) {
      Long value = raftGroupLeadersCountMap.get(raftPeerId);
      if (prevVal == null) {
        prevVal = value;
      } else if (value > prevVal) {
        return false;
      }
    }
    return true;
  }
}
