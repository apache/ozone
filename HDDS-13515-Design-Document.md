# HDDS-13515: Staged Reprocessing for Recon Task Data During Full Snapshot Recovery

## Document Information
- **JIRA**: HDDS-13515
- **Component**: Apache Ozone - Recon
- **Type**: Design Document
- **Author**: [Author Name]
- **Date**: [Current Date]
- **Status**: Draft

---

## 1. Executive Summary

### Problem Statement
Currently, when Recon falls back to full snapshot recovery (due to SequenceNumberNotFoundException from OM compaction), the `reprocess()` method of all ReconOmTask implementations truncates their existing processed data tables before rebuilding from the new snapshot. This causes Recon APIs to return empty/blank data during the reprocessing period, creating a poor user experience in the Recon UI until all tasks complete their full rebuild.

### Proposed Solution
Implement a **Staged Reprocessing Architecture** that leverages the existing staging pattern (similar to TarExtractor) to maintain data availability during full snapshot recovery. The solution involves:

1. **Staging Database Creation**: Create staging instances of ReconOmTask data tables
2. **Parallel Reprocessing**: Process full snapshot data into staging tables while production tables remain accessible
3. **Atomic Switchover**: Atomically replace production tables with staging tables once all tasks complete successfully
4. **Rollback Capability**: Provide rollback mechanism in case of reprocessing failures

### Benefits
- **Zero Downtime**: Recon APIs remain functional during full snapshot recovery
- **Data Consistency**: Atomic switchover ensures consistent view across all task data
- **Failure Resilience**: Rollback capability ensures system stability during failures
- **Performance Isolation**: Reprocessing load doesn't impact API query performance

---

## 2. Current Architecture Analysis

### 2.1 Current Sync Flow
```
OM DB Updates → Recon Sync → Delta Processing → Task Events → Task Tables → APIs
                     ↓
              (SequenceNumberNotFoundException)
                     ↓
              Full Snapshot → Reprocess → Clear Tables → Rebuild → APIs
                                           ↑
                                    USER IMPACT ZONE
```

### 2.2 Current ReconOmTask Data Management

#### Task Data Tables Overview
| Task Class | Data Tables | Storage Type | Clear Method | Impact on APIs |
|------------|-------------|--------------|--------------|----------------|
| **NSSummaryTask** | `nsSummaryTable` | RocksDB | `clearNSSummaryTable()` | Namespace browsing, path construction |
| **OmTableInsightTask** | `GlobalStats` (SQL) | SQL | Reinitialize maps | Table statistics, counts |
| **FileSizeCountTaskFSO/OBS** | `FILE_COUNT_BY_SIZE` (SQL) | SQL | `delete().execute()` | File size distribution charts |
| **ContainerKeyMapperTask** | `CONTAINER_KEY`, `KEY_CONTAINER`, `CONTAINER_KEY_COUNT` | RocksDB | `reinitWithNewContainerDataFromOm()` | Container content browsing |

#### Current Reprocess Flow
```java
// Current problematic pattern across all tasks
public TaskResult reprocess(OMMetadataManager omMetadataManager) {
    try {
        // STEP 1: Clear existing data (USER IMPACT STARTS HERE)
        clearExistingData(); 
        
        // STEP 2: Process full snapshot (TAKES SIGNIFICANT TIME)
        processFullSnapshot(omMetadataManager);
        
        // STEP 3: Commit new data (USER IMPACT ENDS HERE)
        return buildTaskResult(true);
    } catch (Exception e) {
        return buildTaskResult(false);
    }
}
```

### 2.3 Current Storage Layer Interfaces

#### ReconNamespaceSummaryManager (RocksDB)
```java
public interface ReconNamespaceSummaryManager {
    // Current operations
    void clearNSSummaryTable() throws IOException;
    void storeNSSummary(NSSummary nsSummary) throws IOException;
    NSSummary getNSSummary(long objectId) throws IOException;
    
    // Batch operations
    void batchStoreNSSummaries(List<NSSummary> nsSummaries);
    void commitBatchOperation(RDBBatchOperation batchOperation);
}
```

#### ReconContainerMetadataManager (RocksDB)
```java
public interface ReconContainerMetadataManager {
    // Current operations
    void reinitWithNewContainerDataFromOm(Map<Long, Long> containerKeyCountMap);
    void batchStoreContainerKeyMapping(Map<ContainerKeyPrefix, Integer> containerKeyMap);
    
    // Query operations
    Set<ContainerKeyPrefix> getKeyPrefixesForContainer(long containerId, String prevKey, int limit);
    long getKeyCountForContainer(long containerId);
}
```

#### SQL-based Storage (GlobalStatsDao, FileCountBySizeDao)
```java
// Current SQL operations
GlobalStats record = globalStatsDao.fetchOneByKey(key);
globalStatsDao.insert(newRecord) or globalStatsDao.update(newRecord);

// Truncation coordination
if (ReconConstants.FILE_SIZE_COUNT_TABLE_TRUNCATED.compareAndSet(false, true)) {
    dslContext.delete(FILE_COUNT_BY_SIZE).execute();
}
```

---

## 3. Proposed Staged Reprocessing Architecture

### 3.1 High-Level Architecture

```
                    Recon Staged Reprocessing Architecture
                           
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Production    │    │   Staging Area   │    │   Management    │
│   Database      │    │                  │    │   Layer         │
└─────────────────┘    └──────────────────┘    └─────────────────┘
         │                        │                        │
         │                        │                        │
    ┌────▼────┐              ┌────▼────┐              ┌────▼────┐
    │ Active  │              │Staging  │              │Staging  │
    │ Tables  │              │Tables   │              │Manager  │
    │         │              │         │              │         │
    │• ns_summary            │• ns_summary_staging    │• State  │
    │• container_key         │• container_key_staging │  Track  │
    │• global_stats          │• global_stats_staging  │• Switch │
    │• file_count_by_size    │• file_count_by_size_   │  Control│
    └─────────┘              │  staging               │• Error  │
         │                   └─────────┘              │  Handle │
         │                        │                   └─────────┘
         │                        │                        │
    ┌────▼────────────────────────▼──────────────────────▼─────┐
    │                API Access Layer                          │
    │         (Routes to Active or Staging based on state)     │
    └──────────────────────────────────────────────────────────┘
```

### 3.2 Core Components

#### 3.2.1 StagingManager Interface
```java
public interface StagingManager {
    /**
     * Create staging area for reprocessing operation
     * @return stagingId unique identifier for this staging operation
     */
    String createStagingArea() throws IOException;
    
    /**
     * Get staging-aware storage interfaces for reprocessing
     * @param stagingId the staging operation identifier
     * @return map of storage interfaces configured for staging
     */
    Map<String, Object> getStagingStorageInterfaces(String stagingId) throws IOException;
    
    /**
     * Atomically switch from production to staging data
     * @param stagingId the staging operation to promote
     * @return true if switch was successful
     */
    boolean commitStagingArea(String stagingId) throws IOException;
    
    /**
     * Rollback staging operation and clean up staging data
     * @param stagingId the staging operation to rollback
     */
    void rollbackStagingArea(String stagingId) throws IOException;
    
    /**
     * Get current staging state for monitoring
     */
    StagingState getStagingState();
}
```

#### 3.2.2 Enhanced Storage Interface Contracts

**Enhanced ReconNamespaceSummaryManager**
```java
public interface ReconNamespaceSummaryManager {
    // Existing operations...
    
    // Staging-aware operations
    void clearNSSummaryTable(String stagingId) throws IOException;
    void storeNSSummary(NSSummary nsSummary, String stagingId) throws IOException;
    NSSummary getNSSummary(long objectId, String stagingId) throws IOException;
    
    // Atomic operations
    void switchToStaging(String stagingId) throws IOException;
    void cleanupStaging(String stagingId) throws IOException;
}
```

**Enhanced ReconContainerMetadataManager**
```java
public interface ReconContainerMetadataManager {
    // Existing operations...
    
    // Staging-aware operations  
    void reinitWithNewContainerDataFromOm(Map<Long, Long> containerKeyCountMap, String stagingId);
    void batchStoreContainerKeyMapping(Map<ContainerKeyPrefix, Integer> containerKeyMap, String stagingId);
    
    // Atomic operations
    void switchToStaging(String stagingId) throws IOException;
    void cleanupStaging(String stagingId) throws IOException;
}
```

### 3.3 Staging Database Implementation Patterns

#### 3.3.1 RocksDB Staging Pattern (Based on TarExtractor)
```java
public class RocksDBStagingManager {
    private static final String STAGING_PREFIX = ".staging_";
    
    public String createStagingDB(String basePath) {
        String stagingId = STAGING_PREFIX + UUID.randomUUID();
        Path stagingPath = Paths.get(basePath).getParent().resolve(stagingId);
        
        // Create staging directory
        Files.createDirectories(stagingPath);
        
        // Initialize empty RocksDB instance
        initializeRocksDB(stagingPath);
        
        return stagingId;
    }
    
    public void atomicSwitchDB(String stagingId, String productionPath) {
        Path stagingPath = getStagingPath(stagingId);
        Path productionPath = Paths.get(productionPath);
        
        // Close production DB connections
        closeProductionDB();
        
        // Atomic filesystem move (similar to TarExtractor pattern)
        if (Files.exists(productionPath)) {
            Path backupPath = createBackupPath();
            Files.move(productionPath, backupPath, StandardCopyOption.ATOMIC_MOVE);
        }
        
        Files.move(stagingPath, productionPath, 
                  StandardCopyOption.ATOMIC_MOVE, 
                  StandardCopyOption.REPLACE_EXISTING);
        
        // Reinitialize production DB connections
        initializeProductionDB();
    }
}
```

#### 3.3.2 SQL Staging Pattern
```java
public class SQLStagingManager {
    public String createStagingTables(String stagingId) {
        // Create staging versions of each table
        executeDDL("CREATE TABLE global_stats_" + stagingId + " AS SELECT * FROM global_stats WHERE 1=0");
        executeDDL("CREATE TABLE file_count_by_size_" + stagingId + " AS SELECT * FROM file_count_by_size WHERE 1=0");
        
        return stagingId;
    }
    
    public void atomicSwitchTables(String stagingId) {
        executeInTransaction(() -> {
            // Rename tables atomically
            executeDDL("ALTER TABLE global_stats RENAME TO global_stats_backup_" + System.currentTimeMillis());
            executeDDL("ALTER TABLE global_stats_" + stagingId + " RENAME TO global_stats");
            
            executeDDL("ALTER TABLE file_count_by_size RENAME TO file_count_by_size_backup_" + System.currentTimeMillis());
            executeDDL("ALTER TABLE file_count_by_size_" + stagingId + " RENAME TO file_count_by_size");
        });
    }
}
```

---

## 4. Detailed Design

### 4.1 Staging State Management

#### 4.1.1 Staging State Enum
```java
public enum StagingState {
    NONE,              // No staging operation in progress
    INITIALIZING,      // Creating staging area and storage interfaces  
    PROCESSING,        // Tasks are reprocessing into staging area
    READY_TO_COMMIT,   // All tasks completed successfully, ready for switch
    COMMITTING,        // Atomic switch in progress
    COMMITTED,         // Switch completed successfully
    ROLLING_BACK,      // Rollback in progress due to failure
    FAILED             // Operation failed, manual intervention needed
}
```

#### 4.1.2 Staging State Transitions
```
NONE → INITIALIZING → PROCESSING → READY_TO_COMMIT → COMMITTING → COMMITTED → NONE
                         ↓               ↓               ↓
                    ROLLING_BACK ← ROLLING_BACK ← ROLLING_BACK
                         ↓
                      FAILED
```

### 4.2 Enhanced ReconOmTask Interface

#### 4.2.1 Staging-Aware Task Interface
```java
public interface ReconOmTask {
    // Existing methods...
    TaskResult process(OMUpdateEventBatch events, Map<String, Integer> subTaskSeekPosMap);
    TaskResult reprocess(OMMetadataManager omMetadataManager);
    
    // New staging-aware methods
    TaskResult reprocessToStaging(OMMetadataManager omMetadataManager, String stagingId);
    boolean validateStagingData(String stagingId);
    void cleanupStagingData(String stagingId);
    
    // Lifecycle hooks
    void onStagingStart(String stagingId);
    void onStagingComplete(String stagingId, boolean success);
}
```

#### 4.2.2 Enhanced Task Implementation Pattern
```java
public class NSSummaryTask implements ReconOmTask {
    
    @Override
    public TaskResult reprocessToStaging(OMMetadataManager omMetadataManager, String stagingId) {
        long startTime = System.nanoTime();
        
        try {
            // Clear staging table (not production)
            reconNamespaceSummaryManager.clearNSSummaryTable(stagingId);
            
            // Process data into staging
            Collection<Callable<Boolean>> tasks = createStagingSafeTasks(omMetadataManager, stagingId);
            
            ExecutorService executorService = createExecutorService();
            List<Future<Boolean>> results = executorService.invokeAll(tasks);
            
            // Validate all tasks succeeded
            for (Future<Boolean> result : results) {
                if (!result.get()) {
                    LOG.error("NSSummary staging reprocess failed for one of the sub-tasks.");
                    return buildTaskResult(false);
                }
            }
            
            // Validate staging data integrity
            if (!validateStagingData(stagingId)) {
                LOG.error("Staging data validation failed for NSSummaryTask");
                return buildTaskResult(false);
            }
            
            return buildTaskResult(true);
            
        } catch (Exception e) {
            LOG.error("Error during NSSummary staging reprocess", e);
            cleanupStagingData(stagingId);
            return buildTaskResult(false);
        } finally {
            executorService.shutdown();
            logPerformanceMetrics(startTime);
        }
    }
    
    private Collection<Callable<Boolean>> createStagingSafeTasks(OMMetadataManager omMetadataManager, String stagingId) {
        Collection<Callable<Boolean>> tasks = new ArrayList<>();
        
        tasks.add(() -> nsSummaryTaskWithFSO.reprocessWithFSO(omMetadataManager, stagingId));
        tasks.add(() -> nsSummaryTaskWithLegacy.reprocessWithLegacy(reconOMMetadataManager, stagingId));
        tasks.add(() -> nsSummaryTaskWithOBS.reprocessWithOBS(reconOMMetadataManager, stagingId));
        
        return tasks;
    }
    
    @Override
    public boolean validateStagingData(String stagingId) {
        try {
            // Validate staging data completeness and consistency
            long stagingCount = reconNamespaceSummaryManager.getTableSize(stagingId);
            return stagingCount > 0; // Basic validation
        } catch (Exception e) {
            LOG.error("Failed to validate staging data", e);
            return false;
        }
    }
    
    @Override 
    public void cleanupStagingData(String stagingId) {
        try {
            reconNamespaceSummaryManager.cleanupStaging(stagingId);
        } catch (Exception e) {
            LOG.warn("Failed to cleanup staging data for {}", stagingId, e);
        }
    }
}
```

### 4.3 Orchestrated Staging Reprocess Flow

#### 4.3.1 Enhanced ReconTaskController
```java
public class ReconTaskControllerImpl implements ReconTaskController {
    
    private StagingManager stagingManager;
    private volatile StagingState currentStagingState = StagingState.NONE;
    
    /**
     * Enhanced reInitializeTasks with staging support
     */
    @Override
    public synchronized void reInitializeTasksWithStaging(ReconOMMetadataManager omMetadataManager,
                                                         Map<String, ReconOmTask> reconOmTaskMap) {
        if (currentStagingState != StagingState.NONE) {
            LOG.warn("Staging operation already in progress: {}", currentStagingState);
            return;
        }
        
        String stagingId = null;
        try {
            // Phase 1: Initialize staging area
            currentStagingState = StagingState.INITIALIZING;
            stagingId = stagingManager.createStagingArea();
            Map<String, Object> stagingInterfaces = stagingManager.getStagingStorageInterfaces(stagingId);
            
            // Phase 2: Execute staging reprocess
            currentStagingState = StagingState.PROCESSING;
            boolean allTasksSucceeded = executeStagingreprocess(omMetadataManager, reconOmTaskMap, stagingId);
            
            if (!allTasksSucceeded) {
                throw new RuntimeException("One or more tasks failed during staging reprocess");
            }
            
            // Phase 3: Validate staging data
            currentStagingState = StagingState.READY_TO_COMMIT;
            if (!validateAllStagingData(reconOmTaskMap, stagingId)) {
                throw new RuntimeException("Staging data validation failed");
            }
            
            // Phase 4: Atomic commit
            currentStagingState = StagingState.COMMITTING;
            boolean commitSuccess = stagingManager.commitStagingArea(stagingId);
            
            if (!commitSuccess) {
                throw new RuntimeException("Failed to commit staging area");
            }
            
            currentStagingState = StagingState.COMMITTED;
            LOG.info("Staging reprocess completed successfully with stagingId: {}", stagingId);
            
            // Notify all tasks of successful staging completion
            notifyTasksOfStagingCompletion(reconOmTaskMap, stagingId, true);
            
        } catch (Exception e) {
            LOG.error("Staging reprocess failed", e);
            currentStagingState = StagingState.ROLLING_BACK;
            
            try {
                if (stagingId != null) {
                    stagingManager.rollbackStagingArea(stagingId);
                    notifyTasksOfStagingCompletion(reconOmTaskMap, stagingId, false);
                }
            } catch (Exception rollbackException) {
                LOG.error("Failed to rollback staging area", rollbackException);
                currentStagingState = StagingState.FAILED;
                return;
            }
        } finally {
            if (currentStagingState != StagingState.FAILED) {
                currentStagingState = StagingState.NONE;
            }
        }
    }
    
    private boolean executeStagingreprocess(ReconOMMetadataManager omMetadataManager,
                                          Map<String, ReconOmTask> reconOmTaskMap,
                                          String stagingId) {
        Collection<NamedCallableTask<TaskResult>> tasks = new ArrayList<>();
        
        Map<String, ReconOmTask> tasksToProcess = (reconOmTaskMap != null) ? reconOmTaskMap : reconOmTasks;
        
        // Create staging-aware tasks
        tasksToProcess.values().forEach(task -> {
            ReconTaskStatusUpdater taskStatusUpdater = taskStatusUpdaterManager.getTaskStatusUpdater(task.getTaskName());
            taskStatusUpdater.recordRunStart();
            
            tasks.add(new NamedCallableTask<>(task.getTaskName(), 
                () -> task.reprocessToStaging(omMetadataManager, stagingId)));
        });
        
        // Execute all tasks in parallel
        List<CompletableFuture<Void>> futures = tasks.stream()
            .map(task -> CompletableFuture.supplyAsync(() -> {
                try {
                    return task.call();
                } catch (Exception e) {
                    throw new TaskExecutionException(task.getTaskName(), e);
                }
            }, executorService).thenAccept(result -> {
                String taskName = result.getTaskName();
                ReconTaskStatusUpdater taskStatusUpdater = taskStatusUpdaterManager.getTaskStatusUpdater(taskName);
                
                if (!result.isTaskSuccess()) {
                    LOG.error("Staging reprocess failed for task {}", taskName);
                    taskStatusUpdater.setLastTaskRunStatus(-1);
                } else {
                    taskStatusUpdater.setLastTaskRunStatus(0);
                    taskStatusUpdater.setLastUpdatedSeqNumber(omMetadataManager.getLastSequenceNumberFromDB());
                }
                taskStatusUpdater.recordRunCompletion();
            }).exceptionally(ex -> {
                LOG.error("Task failed with exception during staging: ", ex);
                return null;
            }))
            .collect(Collectors.toList());
        
        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
            return true;
        } catch (Exception e) {
            LOG.error("One or more staging tasks failed", e);
            return false;
        }
    }
    
    private boolean validateAllStagingData(Map<String, ReconOmTask> reconOmTaskMap, String stagingId) {
        Map<String, ReconOmTask> tasksToValidate = (reconOmTaskMap != null) ? reconOmTaskMap : reconOmTasks;
        
        for (ReconOmTask task : tasksToValidate.values()) {
            if (!task.validateStagingData(stagingId)) {
                LOG.error("Staging data validation failed for task: {}", task.getTaskName());
                return false;
            }
        }
        return true;
    }
    
    private void notifyTasksOfStagingCompletion(Map<String, ReconOmTask> reconOmTaskMap, String stagingId, boolean success) {
        Map<String, ReconOmTask> tasksToNotify = (reconOmTaskMap != null) ? reconOmTaskMap : reconOmTasks;
        
        tasksToNotify.values().forEach(task -> {
            try {
                task.onStagingComplete(stagingId, success);
            } catch (Exception e) {
                LOG.warn("Task {} failed to handle staging completion notification", task.getTaskName(), e);
            }
        });
    }
    
    public StagingState getCurrentStagingState() {
        return currentStagingState;
    }
}
```

### 4.4 API Layer Modifications

#### 4.4.1 Staging-Aware API Access Pattern
```java
@RestController
public class NSSummaryEndpoint {
    
    @Autowired
    private ReconTaskController reconTaskController;
    
    @GetMapping("/namespace/summary")
    public ResponseEntity<NSSummary> getNSSummary(@RequestParam long objectId) {
        // Check if staging operation is in progress
        StagingState stagingState = reconTaskController.getCurrentStagingState();
        
        try {
            NSSummary summary;
            switch (stagingState) {
                case NONE:
                case COMMITTED:
                    // Normal operation - use production data
                    summary = reconNamespaceSummaryManager.getNSSummary(objectId);
                    break;
                    
                case PROCESSING:
                case READY_TO_COMMIT:
                case COMMITTING:
                    // Staging in progress - continue using production data
                    summary = reconNamespaceSummaryManager.getNSSummary(objectId);
                    // Add staging indicator header
                    response.addHeader("X-Recon-Staging-State", stagingState.toString());
                    break;
                    
                case ROLLING_BACK:
                case FAILED:
                    // Error scenarios - use production data with warning
                    summary = reconNamespaceSummaryManager.getNSSummary(objectId);
                    response.addHeader("X-Recon-Warning", "Staging operation failed, data may be inconsistent");
                    break;
                    
                default:
                    throw new ServiceNotReadyException("Unknown staging state: " + stagingState);
            }
            
            return ResponseEntity.ok(summary);
            
        } catch (Exception e) {
            LOG.error("Failed to retrieve namespace summary", e);
            throw new ServiceNotReadyException("Service temporarily unavailable");
        }
    }
}
```

---

## 5. Implementation Scenarios

### 5.1 Success Scenario: Smooth Staging Operation

#### Timeline
```
T0: SequenceNumberNotFoundException occurs → Full snapshot triggered
T1: Staging area creation begins
    - Create staging RocksDB instances  
    - Create staging SQL tables
    - Initialize staging storage interfaces
    
T2: Parallel staging reprocess begins (Production APIs remain functional)
    - NSSummaryTask → staging_ns_summary_uuid
    - ContainerKeyMapperTask → staging_container_key_uuid  
    - FileSizeCountTask → staging_file_count_by_size_uuid
    - OmTableInsightTask → staging_global_stats_uuid
    
T3: All tasks complete staging reprocess successfully
    - Staging data validation passes
    - System ready for atomic switch
    
T4: Atomic switchover (Brief API unavailability ~seconds)
    - RocksDB: Atomic directory moves
    - SQL: Atomic table renames in transaction
    - Storage interface reinitialization
    
T5: System operational with fresh data
    - All APIs using new processed data
    - Staging cleanup completed
    - Old backup data retained for rollback if needed
```

#### User Experience
- **T0-T4**: Recon UI continues to show existing data (slightly stale but functional)
- **T4**: Brief loading indicators during atomic switch (~1-5 seconds)
- **T5+**: Fresh data from new OM snapshot available

### 5.2 Failure Scenario: Task Failure During Staging

#### Timeline
```
T0: SequenceNumberNotFoundException occurs → Full snapshot triggered
T1: Staging area creation successful
T2: Staging reprocess begins
T3: One task fails (e.g., NSSummaryTask encounters corruption)
    - Task failure detected
    - Rollback procedure initiated
    - Other tasks stopped gracefully
    
T4: Cleanup completed
    - Staging data deleted
    - System returns to previous state
    - Error logged for investigation
    
T5: Retry mechanism or manual intervention
    - Automatic retry after delay (configurable)
    - Or manual intervention based on error type
```

#### User Experience
- **T0-T3**: Normal operation continues with existing data
- **T4+**: System continues with previous data, error logged
- **Admin notification**: Alert sent for manual investigation

### 5.3 Failure Scenario: Atomic Switch Failure

#### Timeline
```
T0-T3: Normal staging process completes successfully
T4: Atomic switch begins but fails (e.g., filesystem error, DB lock)
    - Partial switch detected
    - Immediate rollback initiated
    - Production data restored from backup
    
T5: System recovery
    - Production services restored
    - Staging data preserved for analysis
    - Fallback to old data until next retry
```

#### User Experience
- **T0-T4**: Normal operation continues
- **T4**: Brief service disruption (seconds to minutes)
- **T5+**: Service restored with previous data, retry scheduled

### 5.4 High Load Scenario: Large Dataset Processing

#### Timeline
```
T0: Full snapshot with 100M+ keys triggered
T1: Staging area created with enhanced resources
    - Increased memory allocation for staging tasks
    - Separate thread pools for staging vs production
    
T2: Intelligent processing strategies
    - Batch size optimization based on available memory
    - Periodic progress reporting
    - Circuit breaker for resource exhaustion
    
T3: Extended processing time (30-60 minutes)
    - Production APIs remain responsive
    - Staging progress monitored and reported
    - Resource utilization tracked
    
T4: Successful completion and switch
    - Large dataset successfully processed
    - Atomic switch with minimal downtime
```

#### User Experience
- **T0-T4**: Normal operation with progress indicators in admin UI
- **T4**: Standard brief switch period
- **T5+**: Fresh data available with improved performance

---

## 6. Configuration and Monitoring

### 6.1 Configuration Parameters

```properties
# Staging Configuration
ozone.recon.staging.enabled=true
ozone.recon.staging.base.path=/opt/ozone/recon/staging
ozone.recon.staging.cleanup.retention.hours=24
ozone.recon.staging.timeout.minutes=120

# Resource Allocation
ozone.recon.staging.memory.factor=0.5
ozone.recon.staging.thread.pool.size=4
ozone.recon.staging.batch.size.multiplier=2.0

# Rollback and Retry
ozone.recon.staging.auto.retry.enabled=true  
ozone.recon.staging.max.retry.attempts=3
ozone.recon.staging.retry.delay.minutes=10

# Monitoring
ozone.recon.staging.progress.report.interval.seconds=30
ozone.recon.staging.health.check.interval.seconds=10
```

### 6.2 Metrics and Monitoring

#### Staging Metrics
```java
public class StagingMetrics {
    // Performance metrics
    private final Timer stagingDuration;
    private final Timer atomicSwitchDuration;
    private final Gauge currentStagingState;
    
    // Progress metrics
    private final Gauge stagingProgressPercent;
    private final Counter stagingOperationsTotal;
    private final Counter stagingOperationsSuccessful;
    private final Counter stagingOperationsFailures;
    
    // Resource metrics
    private final Gauge stagingDiskUsage;
    private final Gauge stagingMemoryUsage;
    private final Gauge stagingThreadCount;
    
    // Task-specific metrics
    private final Map<String, Gauge> taskStagingProgress;
    private final Map<String, Timer> taskStagingDuration;
}
```

#### Health Checks
```java
public class StagingHealthIndicator implements HealthIndicator {
    @Override
    public Health health() {
        StagingState state = stagingManager.getStagingState();
        
        switch (state) {
            case NONE:
            case COMMITTED:
                return Health.up()
                    .withDetail("staging", "idle")
                    .build();
                    
            case PROCESSING:
                return Health.up()
                    .withDetail("staging", "in-progress")
                    .withDetail("progress", getProgressPercent())
                    .build();
                    
            case FAILED:
                return Health.down()
                    .withDetail("staging", "failed")
                    .withDetail("error", getLastError())
                    .build();
                    
            default:
                return Health.unknown()
                    .withDetail("staging", state.toString())
                    .build();
        }
    }
}
```

### 6.3 Administrative APIs

#### Staging Control APIs
```java
@RestController
@RequestMapping("/admin/staging")
public class StagingAdminController {
    
    @GetMapping("/status")
    public StagingStatusResponse getStagingStatus() {
        return StagingStatusResponse.builder()
            .state(stagingManager.getStagingState())
            .progress(stagingManager.getProgressDetails())
            .startTime(stagingManager.getStartTime())
            .estimatedCompletion(stagingManager.getEstimatedCompletion())
            .taskProgress(stagingManager.getTaskProgress())
            .build();
    }
    
    @PostMapping("/cancel")
    public ResponseEntity<String> cancelStaging() {
        try {
            boolean cancelled = stagingManager.cancelCurrentOperation();
            return cancelled ? 
                ResponseEntity.ok("Staging operation cancelled") :
                ResponseEntity.badRequest().body("No staging operation to cancel");
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body("Failed to cancel: " + e.getMessage());
        }
    }
    
    @PostMapping("/retry")
    public ResponseEntity<String> retryStaging() {
        try {
            stagingManager.retryLastFailedOperation();
            return ResponseEntity.ok("Staging retry initiated");
        } catch (Exception e) {
            return ResponseEntity.badRequest().body("Retry failed: " + e.getMessage());
        }
    }
}
```

---

## 7. Risk Analysis and Mitigation

### 7.1 Technical Risks

#### Risk 1: Resource Exhaustion During Staging
**Description**: Large datasets may cause memory/disk exhaustion
**Probability**: Medium
**Impact**: High
**Mitigation**:
- Implement resource monitoring and circuit breakers
- Configurable batch sizes and memory limits
- Graceful degradation with partial processing

#### Risk 2: Atomic Switch Failure
**Description**: Filesystem/database issues during switch operation
**Probability**: Low  
**Impact**: High
**Mitigation**:
- Comprehensive pre-switch validation
- Atomic operation with rollback capability
- Multiple backup strategies (filesystem + database backups)

#### Risk 3: Staging Data Corruption
**Description**: Staged data becomes corrupted during processing
**Probability**: Low
**Impact**: Medium
**Mitigation**:
- Staging data validation before commit
- Checksums and integrity verification
- Ability to restart staging from clean state

#### Risk 4: Performance Impact on Production APIs
**Description**: Staging operations affect production API performance
**Probability**: Medium
**Impact**: Medium
**Mitigation**:
- Separate resource pools for staging and production
- Configurable resource allocation limits
- Performance monitoring and alerting

### 7.2 Operational Risks

#### Risk 5: Configuration Complexity
**Description**: Complex configuration leads to operational errors
**Probability**: Medium
**Impact**: Medium
**Mitigation**:
- Comprehensive documentation and examples
- Validation of configuration parameters
- Default values for safe operation

#### Risk 6: Debugging Complexity
**Description**: Staging operations make troubleshooting more complex
**Probability**: Medium
**Impact**: Medium
**Mitigation**:
- Detailed logging at each stage
- Clear state transitions and error messages
- Administrative APIs for visibility

### 7.3 Compatibility Risks

#### Risk 7: Storage Format Evolution
**Description**: Changes to storage formats break staging compatibility
**Probability**: Low
**Impact**: High
**Mitigation**:
- Version compatibility checks in staging manager
- Migration strategies for format changes
- Backward compatibility requirements

---

## 8. Testing Strategy

### 8.1 Unit Testing

#### Component Tests
- **StagingManager**: Mock storage interfaces, test state transitions
- **Enhanced Storage Interfaces**: Test staging operations in isolation
- **Task Implementations**: Test staging-aware reprocess methods

#### Mock-based Testing
```java
@Test
public void testStagingReprocessSuccess() {
    // Setup
    String stagingId = "test-staging-123";
    StagingManager mockStagingManager = mock(StagingManager.class);
    ReconNamespaceSummaryManager mockStorage = mock(ReconNamespaceSummaryManager.class);
    
    when(mockStagingManager.createStagingArea()).thenReturn(stagingId);
    when(mockStorage.validateStagingData(stagingId)).thenReturn(true);
    
    // Execute
    NSSummaryTask task = new NSSummaryTask(mockStorage, mockReconOM, config);
    TaskResult result = task.reprocessToStaging(mockOMMetadataManager, stagingId);
    
    // Verify
    assertTrue(result.isTaskSuccess());
    verify(mockStorage).clearNSSummaryTable(stagingId);
    verify(mockStorage).validateStagingData(stagingId);
}
```

### 8.2 Integration Testing

#### End-to-End Staging Tests
```java
@IntegrationTest
public class StagingIntegrationTest {
    
    @Test
    public void testFullStagingCycle() {
        // Setup test OM snapshot
        OMMetadataManager testOM = createTestOMWithData();
        
        // Trigger staging reprocess
        String stagingId = reconTaskController.reInitializeTasksWithStaging(testOM, null);
        
        // Verify staging state transitions
        await().atMost(Duration.ofMinutes(5))
               .until(() -> stagingManager.getStagingState() == StagingState.COMMITTED);
        
        // Verify data consistency
        verifyDataConsistency(testOM);
        
        // Verify API availability throughout process
        verifyAPIAvailability();
    }
    
    @Test
    public void testStagingRollback() {
        // Setup failing scenario
        NSSummaryTask faultyTask = createFaultyTask();
        reconTaskController.registerTask(faultyTask);
        
        // Trigger staging that should fail
        reconTaskController.reInitializeTasksWithStaging(testOM, null);
        
        // Verify rollback
        await().atMost(Duration.ofMinutes(2))
               .until(() -> stagingManager.getStagingState() == StagingState.NONE);
        
        // Verify production data unchanged
        verifyProductionDataIntact();
    }
}
```

### 8.3 Performance Testing

#### Load Testing Scenarios
1. **Large Dataset Processing**: 100M+ keys with staging
2. **Concurrent API Load**: High API traffic during staging
3. **Resource Constraint Testing**: Limited memory/disk scenarios
4. **Failure Recovery**: Performance after rollback operations

#### Performance Benchmarks
```java
@PerformanceTest
public class StagingPerformanceTest {
    
    @Test
    public void benchmarkStagingVsDirectReprocess() {
        // Measure current reprocess time
        long directReprocessTime = measureDirectReprocess();
        
        // Measure staging reprocess time  
        long stagingReprocessTime = measureStagingReprocess();
        
        // Measure API availability during both approaches
        double directAPIAvailability = measureAPIAvailabilityDuringDirect();
        double stagingAPIAvailability = measureAPIAvailabilityDuringStaging();
        
        // Assert performance criteria
        assertThat(stagingAPIAvailability).isGreaterThan(0.95); // 95% availability
        assertThat(stagingReprocessTime).isLessThan(directReprocessTime * 1.5); // Max 50% overhead
    }
}
```

### 8.4 Chaos Engineering

#### Failure Injection Tests
```java
@ChaosTest
public class StagingChaosTest {
    
    @Test
    public void testDiskSpaceExhaustionDuringStaging() {
        // Start staging operation
        String stagingId = startStagingOperation();
        
        // Inject disk space exhaustion
        diskSpaceChaosService.exhaustDiskSpace();
        
        // Verify graceful handling
        await().atMost(Duration.ofMinutes(1))
               .until(() -> stagingManager.getStagingState() == StagingState.ROLLING_BACK);
        
        // Verify recovery
        diskSpaceChaosService.restoreDiskSpace();
        verifySystemRecovery();
    }
    
    @Test
    public void testNetworkPartitionDuringAtomicSwitch() {
        // Setup staging ready for commit
        prepareReadyToCommitState();
        
        // Inject network partition during switch
        networkChaosService.partitionNetwork();
        
        // Trigger commit
        boolean result = stagingManager.commitStagingArea(stagingId);
        
        // Verify failure handling
        assertFalse(result);
        assertEquals(StagingState.ROLLING_BACK, stagingManager.getStagingState());
    }
}
```

---

## 9. Migration and Rollout Strategy

### 9.1 Feature Flag Implementation

#### Gradual Rollout Configuration
```properties
# Phase 1: Development and Testing
ozone.recon.staging.enabled=false
ozone.recon.staging.development.mode=true

# Phase 2: Beta Testing
ozone.recon.staging.enabled=true
ozone.recon.staging.beta.mode=true
ozone.recon.staging.fallback.enabled=true

# Phase 3: Production Rollout
ozone.recon.staging.enabled=true
ozone.recon.staging.production.mode=true
ozone.recon.staging.monitoring.enhanced=true
```

#### Fallback Mechanism
```java
public class StagingCapableReconTaskController {
    private final boolean stagingEnabled;
    private final ReconTaskController legacyController;
    private final StagingReconTaskController stagingController;
    
    @Override
    public void reInitializeTasks(ReconOMMetadataManager omMetadataManager, 
                                  Map<String, ReconOmTask> reconOmTaskMap) {
        if (stagingEnabled && !forceLegacyMode) {
            try {
                stagingController.reInitializeTasksWithStaging(omMetadataManager, reconOmTaskMap);
            } catch (Exception e) {
                LOG.error("Staging reprocess failed, falling back to legacy mode", e);
                legacyController.reInitializeTasks(omMetadataManager, reconOmTaskMap);
            }
        } else {
            legacyController.reInitializeTasks(omMetadataManager, reconOmTaskMap);
        }
    }
}
```

### 9.2 Rollout Phases

#### Phase 1: Development and Unit Testing (Week 1-4)
- Implement core staging interfaces and manager
- Develop enhanced storage layer interfaces  
- Create unit tests for all components
- Internal testing with synthetic data

#### Phase 2: Integration Testing (Week 5-8)
- Integration with existing Recon components
- End-to-end testing with real OM snapshots
- Performance benchmarking and optimization
- Chaos engineering tests

#### Phase 3: Beta Testing (Week 9-12)
- Deploy to staging environments
- A/B testing with feature flags
- Monitor performance and stability
- Documentation and operational runbooks

#### Phase 4: Production Rollout (Week 13-16)
- Gradual rollout to production clusters
- Monitor key metrics and user experience
- Feedback collection and issue resolution
- Full production deployment

### 9.3 Success Criteria

#### Technical Metrics
- **API Availability**: >99.5% during staging operations
- **Performance Overhead**: <25% increase in reprocess time
- **Resource Utilization**: <50% increase in peak memory usage
- **Failure Recovery**: <1 minute average rollback time

#### User Experience Metrics
- **Zero Blank UI Periods**: No more empty dashboard scenarios
- **Data Freshness**: <5 minutes delay from snapshot to fresh data
- **Error Rate**: <0.1% staging operation failure rate
- **Admin Visibility**: Complete staging progress visibility

---

## 10. Future Enhancements

### 10.1 Advanced Staging Features

#### Incremental Staging
```java
public interface IncrementalStagingManager extends StagingManager {
    /**
     * Create staging area with incremental updates from last known state
     */
    String createIncrementalStagingArea(long fromSequenceNumber) throws IOException;
    
    /**
     * Apply delta updates to existing staging area
     */
    void applyDeltaToStaging(String stagingId, OMUpdateEventBatch events) throws IOException;
}
```

#### Multi-Version Staging
```java
public interface MultiVersionStagingManager extends StagingManager {
    /**
     * Maintain multiple staging versions for A/B testing
     */
    List<String> createMultipleStaging(int versions) throws IOException;
    
    /**
     * Switch to specific staging version
     */
    boolean switchToVersion(String stagingId) throws IOException;
}
```

### 10.2 Cross-Cluster Staging

#### Distributed Staging
- Stage processing across multiple Recon instances
- Coordinated staging for high availability setups
- Cross-datacenter staging replication

#### Staging as a Service
- Dedicated staging infrastructure
- API-driven staging operations
- Centralized staging management

### 10.3 Machine Learning Integration

#### Predictive Staging
- ML models to predict optimal staging timing
- Resource requirement prediction
- Failure probability assessment

#### Intelligent Resource Management
- Dynamic resource allocation based on dataset characteristics
- Adaptive batch sizing and parallelism
- Predictive scaling for staging operations

---

## 11. Conclusion

The Staged Reprocessing Architecture for HDDS-13515 provides a robust solution to eliminate the data availability gap during Recon's full snapshot recovery operations. By leveraging the proven staging pattern from TarExtractor and extending it to all ReconOmTask data tables, we can maintain continuous API availability while ensuring data consistency and system reliability.

### Key Benefits Delivered:
1. **Zero Downtime**: APIs remain functional during reprocessing
2. **Data Consistency**: Atomic switchover ensures consistent state
3. **Failure Resilience**: Comprehensive rollback and retry mechanisms
4. **Performance Isolation**: Staging operations don't impact API performance
5. **Operational Visibility**: Complete monitoring and administrative control

### Implementation Readiness:
- **Low Risk**: Builds on existing proven patterns (TarExtractor)
- **Backward Compatible**: Feature flags enable gradual rollout
- **Well Tested**: Comprehensive testing strategy covers all scenarios
- **Monitoring Ready**: Built-in metrics and health checks
- **Operationally Sound**: Clear procedures for all scenarios

This design provides a production-ready foundation for eliminating one of Recon's most significant user experience issues while maintaining the robustness and reliability expected in enterprise Apache Ozone deployments.