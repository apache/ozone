package org.apache.hadoop.ozone.recon.metrics;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;

import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_TASK_STATUS_STORAGE_DURATION;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_TASK_STATUS_STORAGE_DURATION_DEFAULT;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class ReconTaskStatusCounter {
  private static ReconTaskStatusCounter instance;
  private final long timeoutDuration;

  public enum ReconTasks {
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

  static Map<ReconTasks, Pair<Integer, Integer>> taskStatusCounter= new EnumMap<>(ReconTasks.class);

  public ReconTaskStatusCounter() {
    OzoneConfiguration conf = new OzoneConfiguration();
    timeoutDuration = conf.getTimeDuration(
      OZONE_RECON_TASK_STATUS_STORAGE_DURATION,
      OZONE_RECON_TASK_STATUS_STORAGE_DURATION_DEFAULT,
      TimeUnit.MILLISECONDS
    );

    initializationTime = System.currentTimeMillis();
    for (ReconTasks task: ReconTasks.values()) {
      taskStatusCounter.put(task, Pair.of(0, 0));
    }
  }

  /**
   * Get an instance of <code>this</code> {@link ReconTaskStatusCounter} in order to persist state
   * of the task counters between multiple modules/packages
   * @return an instance of current {@link ReconTaskStatusCounter}
   */
  public static ReconTaskStatusCounter getCurrentInstance() {
    if (null == instance) {
      instance = new ReconTaskStatusCounter();
    }
    return instance;
  }

  public void updateCounter(Class<?> clazz, boolean successful) {
    int successes = taskStatusCounter.get(ReconTasks.valueOf(clazz.getName())).getLeft();
    int failures = taskStatusCounter.get(ReconTasks.valueOf(clazz.getName())).getRight();
    if (successful) {
      taskStatusCounter.put(ReconTasks.valueOf(clazz.getName()), Pair.of(successes + 1, failures));
    }
    else {
      taskStatusCounter.put(ReconTasks.valueOf(clazz.getName()), Pair.of(successes, failures + 1));
    }
  }

  public void updateCounter(String taskName, boolean successful) {
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
   * @throws NullPointerException if the task name provided is not valid
   */
  public Pair<Integer, Integer> getTaskStatusCounts(String taskName)
    throws NullPointerException{
    if ((initializationTime - System.currentTimeMillis()) > timeoutDuration) {
      instance = new ReconTaskStatusCounter();
      return Pair.of(0, 0);
    }

    try {
      return taskStatusCounter.get(ReconTasks.valueOf(taskName));
    } catch (NullPointerException npe) {
      throw new NullPointerException("Couldn't find task with name " + taskName);
    }
  }

  public Map<ReconTasks, Pair<Integer, Integer>> getTaskCounts() {
    return taskStatusCounter;
  }
}
