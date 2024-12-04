package org.apache.hadoop.ozone.recon.api.types;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;


/**
 * Class to represent the API response structure of task status statistics.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ReconTaskStatusCountResponse {

  // The name of the task for which we are getting status
  @JsonProperty("taskName")
  private String taskName;

  // The number of successes associated with the task
  @JsonProperty("successes")
  private int successCount;

  // The number of failures associated with the task
  @JsonProperty("failures")
  private int failureCount;

  public ReconTaskStatusCountResponse(String taskName, int successCount, int failureCount) {
    this.taskName = taskName;
    this.successCount = successCount;
    this.failureCount = failureCount;
  }

  public String getTaskName() { return taskName; }
  public long getSuccessCount() { return successCount; }
  public long getFailureCount() { return failureCount; }
}
