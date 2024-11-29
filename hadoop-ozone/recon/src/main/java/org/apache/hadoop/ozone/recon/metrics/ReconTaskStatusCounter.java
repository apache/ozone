package org.apache.hadoop.ozone.recon.metrics;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_TASK_STATUS_STORAGE_DURATION;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_TASK_STATUS_STORAGE_DURATION_DEFAULT;

import javax.inject.Inject;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class ReconTaskStatusCounter {
  @Inject
  static OzoneConfiguration conf;

  enum ReconTasks {
    ContainerHealthTask,
    ContainerKeyMapperTask,
    ContainerSizeCountTask,
    FileSizeCountTask,
    NSSummaryTask,
    OmDeltaRequest,
    OmTableInsightTask,
    OmSnapshotRequest,
    PipelineSyncTask,
    ReconScmTask
  }
  static long initializationTime = 0L;

  static Map<ReconTasks, Pair<Integer, Integer>> taskStatusCounter= new EnumMap<ReconTasks, Pair<Integer, Integer>>(ReconTasks.class);

  public static void initialize() {
    initializationTime = System.currentTimeMillis();
    for (ReconTasks task: ReconTasks.values()) {
      taskStatusCounter.put(task, Pair.of(0, 0));
    }
  }

  public static void updateCounter(Class<?> clazz, boolean successful) {
    int successes = taskStatusCounter.get(ReconTasks.valueOf(clazz.getName())).getLeft();
    int failures = taskStatusCounter.get(ReconTasks.valueOf(clazz.getName())).getRight();
    if (successful) {
      taskStatusCounter.put(ReconTasks.valueOf(clazz.getName()), Pair.of(successes + 1, failures));
    }
    else {
      taskStatusCounter.put(ReconTasks.valueOf(clazz.getName()), Pair.of(successes, failures + 1));
    }
  }

  public static void updateCounter(String taskName, boolean successful) {
    int successes = taskStatusCounter.get(ReconTasks.valueOf(taskName)).getLeft();
    int failures = taskStatusCounter.get(ReconTasks.valueOf(taskName)).getRight();
    if (successful) {
      taskStatusCounter.put(ReconTasks.valueOf(taskName), Pair.of(successes + 1, failures));
    }
    else {
      taskStatusCounter.put(ReconTasks.valueOf(taskName), Pair.of(successes, failures + 1));
    }
  }

  /**
   * Get the number of successes and failures for a provided task name
   * @param taskName Stores the task name for which we want to fetch the counts
   * @return A {@link Pair} of <code> {successes, failures} for provided task name </code>
   */
  public static Pair<Integer, Integer> getTaskStatusCounts(String taskName) {
    long timeoutDuration = conf.getTimeDuration(
      OZONE_RECON_TASK_STATUS_STORAGE_DURATION,
      OZONE_RECON_TASK_STATUS_STORAGE_DURATION_DEFAULT,
      TimeUnit.MILLISECONDS
    );
    if ((initializationTime - System.currentTimeMillis()) > timeoutDuration) {
      initialize();
      return Pair.of(0, 0);
    }

    return taskStatusCounter.get(ReconTasks.valueOf(taskName));
  }
}
