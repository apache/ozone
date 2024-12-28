package org.apache.hadoop.ozone.recon.tasks.updater;

import com.google.inject.Singleton;
import org.apache.hadoop.ozone.recon.metrics.ReconTaskStatusCounter;
import org.hadoop.ozone.recon.schema.tables.daos.ReconTaskStatusDao;
import org.hadoop.ozone.recon.schema.tables.pojos.ReconTaskStatus;

import javax.inject.Inject;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class provides caching for ReconTaskStatusUpdater instances.
 * For each task we maintain a map of updater instance and provide it to consumers
 * to update.
 * Here we also make a single call to the TASK_STATUS_TABLE to check if previous values are present
 * for a task in the DB to avoid overwrite to initial state
 */
@Singleton
public class ReconTaskStatusUpdaterManager {
  private final ReconTaskStatusDao reconTaskStatusDao;
  private final ReconTaskStatusCounter reconTaskStatusCounter;
  // Act as a cache for the task updater instances
  private final ConcurrentHashMap<String, ReconTaskStatusUpdater> updaterCache;

  @Inject
  public ReconTaskStatusUpdaterManager(ReconTaskStatusDao reconTaskStatusDao,
                                       ReconTaskStatusCounter reconTaskStatusCounter) {
    this.reconTaskStatusDao = reconTaskStatusDao;
    this.reconTaskStatusCounter = reconTaskStatusCounter;
    this.updaterCache = new ConcurrentHashMap<>();

    // Fetch the tasks present in the DB already
    List<ReconTaskStatus> tasks = reconTaskStatusDao.findAll();
    for (ReconTaskStatus task: tasks) {
      updaterCache.put(task.getTaskName(),
          new ReconTaskStatusUpdater(reconTaskStatusDao, reconTaskStatusCounter, task.getTaskName(),
              task.getLastUpdatedTimestamp(), task.getLastUpdatedSeqNumber(),
              task.getLastTaskRunStatus(), task.getIsCurrentTaskRunning()
          ));
    }
  }

  /**
   * Gets the updater for the provided task name and updates DB with initial values
   * if the task is not already present in DB.
   * @param taskName The name of the task for which we want to get instance of the updater
   * @return An instance of {@link ReconTaskStatusUpdater} for the provided task name.
   */
  public ReconTaskStatusUpdater getTaskStatusUpdater(String taskName) {
    // If the task is not already present in the DB then we can initialize using initial values
    return updaterCache.computeIfAbsent(taskName, (name) -> {
      ReconTaskStatusUpdater taskStatusUpdater = new ReconTaskStatusUpdater(
          reconTaskStatusDao, reconTaskStatusCounter, name);
      // Insert initial values into DB
      taskStatusUpdater.updateDetails();
      return taskStatusUpdater;
    });
  }
}
