# **Transaction Inconsistency Detection in Ratis**

## **1\. Overview**

This document proposes a new feature for the Apache Ratis framework to proactively detect and prevent data inconsistency across leader and follower state machines. The core of this proposal is a periodic, checksum-based consistency check initiated by the Ratis leader, typically during the snapshot process. If an inconsistency is detected, the affected state machine will halt immediately, preventing further divergence and protecting the system from data corruption.

### **1.1 Motivation**

This analysis was born out of debugging critical issues where a definitive source of truth for the database state was unavailable. During the incident, direct access to the Raft logs, which would provide a serialized history of all committed transactions, was not possible.

Furthermore, initial attempts to use the OM audit logs as a ground truth proved unreliable. The audit logs were found to have significant gaps or "holes," meaning they did not contain a complete record of all client-initiated operations.

This lack of a reliable transaction history made it impossible to determine with certainty what data should exist on each OM node. Without knowing precisely what has been flushed to the DB and which transactions have been successfully applied on each system, any divergence between the nodes could not be confidently resolved. This validation plan would overcome these challenges by establishing a ground truth from the available data and systematically identifying true inconsistencies.

---

## **2\. Core Mechanism**

### **2.1. Checksum Generation on Apply Transaction**

To enable validation, the state machine's behavior during `applyTransaction` must be enhanced.

* For every transaction it applies, the state machine should compute a **deterministic checksum** of its entire state up to and including that transaction.  
* This checksum should be included as part of the response from the state machine to the Ratis framework after the transaction is applied.

### **2.2. The `CHECK_SM_CONSISTENCY` Command**

A new internal Ratis command will be introduced to trigger the validation process.

* **Command Name:** `CHECK_SM_CONSISTENCY`  
* **Trigger:** The Ratis leader will initiate this command whenever it takes a new snapshot.  
* **Request Body:** The command will be appended to the Raft log with a payload containing the leader's state information: `<snapshotTransactionId>:<leader_checksum_at_that_transaction>`

### **2.3. Validation and Failure Protocol**

Since the `CHECK_SM_CONSISTENCY` command is written to the Raft log, it will be replicated and processed by all state machines in the quorum (both leader and followers).

1. Upon receiving this command, each state machine should look up its own persisted checksum corresponding to the `transactionId` provided in the request.  
2. It will compare its local checksum against the leader's checksum from the command's payload.  
3. **On Mismatch:** If the checksums do not match, it signals a divergence in the state machine's history. To prevent any further corruption, the state machine **must stop its execution immediately**. This critical failure stops the Ratis server and flags a serious issue requiring manual intervention.

---

## **3\. Data Persistence and Pruning**

To facilitate these checks, state machines must persist a trail of transaction checksums.

* **Data Structure:** A key-value store mapping `transactionId` to its corresponding `checksum`.  
* **Retention Policy:** A full, indefinite history is not required. The checksum history only needs to be maintained for transactions that fall within the log retention window.  
* **Pruning:** The history can be pruned on a rolling basis. Obsolete checksum information can be deleted based on the `raft.server.log.purge.gap` configuration. For example, entries with a `transactionId` older than `(<currentTransactionId> - raft.server.log.purge.gap)` can be safely removed.

---

## **4\. Reference Implementation: Ozone**

The HA implementations of Ozone Manager (OM) and Storage Container Manager (SCM) can adopt this design using RocksDB.

* **Table Name:** A new RocksDB table, `flushedTransactionTable`, will be created.  
  * **Key:** Ratis `transactionId`  
  * **Value:** The computed checksum for the state machine at that transaction.  
* **Persistence:** This table will be persisted alongside other state machine data.  
* **Pruning Implementation:** During a RocksDB batch write operation, a `deleteRange` command can be issued on the `flushedTransactionTable`. This command will efficiently delete all key-value pairs with transaction IDs older than the configured purge gap, ensuring minimal performance overhead.

### 

### **Benefits of this Implementation**

* **Proactive Corruption Prevention:** Halts the system upon detecting the first sign of divergence, avoiding widespread data inconsistency.  
* **Enhanced Debugging:** The `flushedTransactionTable` would also act as an audit trail across nodes give one the ability to pinpoint exactly when and where a divergence or anomaly occurred.