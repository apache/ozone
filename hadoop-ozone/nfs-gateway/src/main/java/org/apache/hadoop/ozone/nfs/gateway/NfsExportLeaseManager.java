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

package org.apache.hadoop.ozone.nfs.gateway;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.ByteString;
import java.io.Closeable;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.RequestContext;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.NfsExportLeaseRequest.Operation;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.NfsExportLeaseResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.NfsFileCommitRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.NfsFileCreateResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.NfsFileLeaseRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.NfsFileLeaseResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.NfsTransferRangeRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.NfsTransferRangeResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Owns the OM export leases for one gateway process and fails closed when a renewal fails. */
final class NfsExportLeaseManager implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(NfsExportLeaseManager.class);
  private static final long EXPORT_LEASE_ACQUIRE_RETRY_INTERVAL_MILLIS = 500;
  private static final long EXPORT_LEASE_ACQUIRE_RETRY_TIMEOUT_MILLIS = TimeUnit.MINUTES.toMillis(1) + 5000;
  private final Map<ByteString, NfsObjectReader> readers;
  private final String gatewayId;
  private final long leaseDurationMillis;
  private final ConcurrentMap<ByteString, LeaseState> leases = new ConcurrentHashMap<>();
  private final ConcurrentMap<FileLeaseKey, FileLeaseState> fileLeases = new ConcurrentHashMap<>();
  private final ScheduledExecutorService renewer = Executors.newSingleThreadScheduledExecutor(
      new ThreadFactoryBuilder().setNameFormat("NfsExportLease-%d").setDaemon(true).build());

  NfsExportLeaseManager(Map<ByteString, NfsObjectReader> readers, String gatewayId, long leaseDurationMillis) {
    if (readers == null || readers.isEmpty() || gatewayId == null || gatewayId.isEmpty()
        || leaseDurationMillis < 3000 || leaseDurationMillis > 60_000) {
      throw new IllegalArgumentException("Invalid NFS export lease manager configuration");
    }
    this.readers = Map.copyOf(readers);
    this.gatewayId = gatewayId;
    this.leaseDurationMillis = leaseDurationMillis;
  }

  void start() throws IOException {
    Map<ByteString, LeaseState> acquired = new HashMap<>();
    try {
      for (Map.Entry<ByteString, NfsObjectReader> entry : readers.entrySet()) {
        NfsExportLeaseResponse response = acquireExportLease(entry.getValue(), entry.getKey());
        acquired.put(entry.getKey(), state(response));
      }
      leases.putAll(acquired);
      long renewalPeriodMillis = Math.max(500, leaseDurationMillis / 3);
      renewer.scheduleAtFixedRate(this::renewAll, renewalPeriodMillis, renewalPeriodMillis,
          TimeUnit.MILLISECONDS);
    } catch (IOException | RuntimeException ex) {
      acquired.forEach((key, state) -> release(key, state));
      renewer.shutdownNow();
      if (ex instanceof IOException) {
        throw (IOException) ex;
      }
      throw ex;
    }
  }

  private NfsExportLeaseResponse acquireExportLease(NfsObjectReader reader, ByteString exportUuid)
      throws IOException {
    long deadlineNanos = System.nanoTime()
        + TimeUnit.MILLISECONDS.toNanos(EXPORT_LEASE_ACQUIRE_RETRY_TIMEOUT_MILLIS);
    boolean waitingForPreviousOwner = false;
    while (true) {
      try {
        return reader.updateExportLease(Operation.ACQUIRE, gatewayId, leaseDurationMillis, null, null);
      } catch (IOException ex) {
        if (!isExportLeaseConflict(ex) || System.nanoTime() >= deadlineNanos) {
          throw ex;
        }
        if (!waitingForPreviousOwner) {
          LOG.info("Waiting for the previous NFS gateway lease to expire for export {}", exportUuid);
          waitingForPreviousOwner = true;
        }
        try {
          Thread.sleep(EXPORT_LEASE_ACQUIRE_RETRY_INTERVAL_MILLIS);
        } catch (InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          throw new IOException("Interrupted while waiting for the previous NFS gateway lease", interrupted);
        }
      }
    }
  }

  private static boolean isExportLeaseConflict(IOException exception) {
    for (Throwable cause = exception; cause != null; cause = cause.getCause()) {
      if (cause instanceof OMException
          && ((OMException) cause).getResult() == OMException.ResultCodes.NFS_LEASE_CONFLICT) {
        return true;
      }
    }
    return false;
  }

  boolean isActive(ByteString exportUuid) {
    LeaseState state = leases.get(exportUuid);
    return state != null && System.nanoTime() < state.expiresAtNanos;
  }

  synchronized void registerFileLease(ByteString exportUuid, String transferId, String principal,
      NfsFileCreateResponse response) throws IOException {
    LeaseState export = leases.get(exportUuid);
    if (export == null || !isActive(exportUuid) || transferId == null || transferId.isEmpty()
        || response.getGeneration() == 0 || response.getObjectID() == 0 || response.getOwnerToken().isEmpty()
        || principal == null || principal.isEmpty() || response.getGatewayGeneration() != export.gatewayGeneration
        || response.getExpiresAtMillis() <= System.currentTimeMillis()) {
      throw new IOException("NFS file lease does not belong to the active export owner");
    }
    FileLeaseKey key = new FileLeaseKey(exportUuid, transferId);
    if (fileLeases.containsKey(key)) {
      throw new IOException("NFS transfer already has an active file lease");
    }
    long fileLeaseDurationMillis = Math.max(1, leaseDurationMillis * 2 / 3);
    NfsFileLeaseResponse renewed = readers.get(exportUuid).updateFileLease(NfsFileLeaseRequest.Operation.RENEW,
        gatewayId, export.gatewayGeneration, response.getObjectID(), transferId, response.getGeneration(),
        response.getOwnerToken(), fileLeaseDurationMillis);
    long remainingMillis = renewed.getExpiresAtMillis() - System.currentTimeMillis();
    if (remainingMillis <= 0) {
      throw new IOException("NFS file lease renewal returned an expired lease");
    }
    FileLeaseState state = new FileLeaseState(response.getObjectID(), transferId, renewed.getGeneration(),
        response.getOwnerToken(), principal, System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(remainingMillis),
        renewed.getContentPublished(), new ConcurrentSkipListMap<>());
    if (fileLeases.putIfAbsent(key, state) != null) {
      throw new IOException("NFS transfer already has an active file lease");
    }
  }

  NfsFileCreateResponse createFile(ByteString exportUuid, RequestContext context, ByteString parentHandle,
      String name, String transferId) throws IOException {
    LeaseState export = leases.get(exportUuid);
    if (export == null || !isActive(exportUuid) || transferId == null || transferId.isEmpty()) {
      throw new IOException("NFS export lease is not active");
    }
    long fileLeaseDurationMillis = Math.max(1, leaseDurationMillis * 2 / 3);
    NfsFileCreateResponse response = readers.get(exportUuid).createFile(context, parentHandle, name, gatewayId,
        export.gatewayGeneration, transferId, fileLeaseDurationMillis);
    try {
      registerFileLease(exportUuid, transferId, readers.get(exportUuid).getCallerUserName(context), response);
      return response;
    } catch (IOException | RuntimeException ex) {
      try {
        readers.get(exportUuid).updateFileLease(NfsFileLeaseRequest.Operation.RELEASE, gatewayId,
            export.gatewayGeneration, response.getObjectID(), transferId, response.getGeneration(),
            response.getOwnerToken(), 0);
      } catch (IOException | RuntimeException releaseFailure) {
        ex.addSuppressed(releaseFailure);
      }
      throw ex;
    }
  }

  boolean isFileLeaseActive(ByteString exportUuid, String transferId) {
    FileLeaseState state = fileLeases.get(new FileLeaseKey(exportUuid, transferId));
    return state != null && System.nanoTime() < state.expiresAtNanos && isActive(exportUuid);
  }

  synchronized NfsFileLeaseResponse recoverFileLease(ByteString exportUuid, long objectId, String principal)
      throws IOException {
    LeaseState export = leases.get(exportUuid);
    if (export == null || !isActive(exportUuid) || objectId == 0 || principal == null || principal.isEmpty()) {
      throw new IOException("NFS export lease or recovery identity is not active");
    }
    for (Map.Entry<FileLeaseKey, FileLeaseState> entry : fileLeases.entrySet()) {
      FileLeaseState existing = entry.getValue();
      if (entry.getKey().exportUuid.equals(exportUuid) && existing.objectId == objectId) {
        long remainingMillis = TimeUnit.NANOSECONDS.toMillis(existing.expiresAtNanos - System.nanoTime());
        if (remainingMillis > 0) {
          if (!existing.principal.equals(principal)) {
            throw new OMException("NFS file lease is not active for the caller",
                OMException.ResultCodes.NFS_LEASE_CONFLICT);
          }
          return NfsFileLeaseResponse.newBuilder().setGeneration(existing.generation)
              .setExpiresAtMillis(System.currentTimeMillis() + remainingMillis).setTransferID(existing.transferId)
              .setOwnerToken(existing.ownerToken).setOwnerPrincipal(existing.principal)
              .setContentPublished(existing.contentPublished).build();
        }
        fileLeases.remove(entry.getKey(), existing);
      }
    }
    long durationMillis = Math.max(1, leaseDurationMillis * 2 / 3);
    if (TimeUnit.NANOSECONDS.toMillis(export.expiresAtNanos - System.nanoTime()) <= durationMillis + 250) {
      NfsExportLeaseResponse renewed = readers.get(exportUuid).updateExportLease(Operation.RENEW, gatewayId,
          leaseDurationMillis, export.gatewayGeneration, export.generation);
      export = state(renewed);
      leases.put(exportUuid, export);
    }
    NfsFileLeaseResponse response = readers.get(exportUuid).updateFileLease(NfsFileLeaseRequest.Operation.RECOVER,
        gatewayId, export.gatewayGeneration, objectId, "", 0, ByteString.EMPTY, durationMillis, principal);
    if (!response.hasTransferID() || !response.hasOwnerToken() || !response.hasOwnerPrincipal()
        || !principal.equals(response.getOwnerPrincipal()) || response.getTransferID().isEmpty()
        || response.getOwnerToken().isEmpty()) {
      throw new IOException("OM returned incomplete NFS file lease recovery state");
    }
    long remainingMillis = response.getExpiresAtMillis() - System.currentTimeMillis();
    if (remainingMillis <= 0) {
      throw new IOException("NFS file lease recovery returned an expired lease");
    }
    FileLeaseKey key = new FileLeaseKey(exportUuid, response.getTransferID());
    ConcurrentSkipListMap<Long, NfsStagingStore.StagedRange> recoveredRanges = lookupStagedRanges(exportUuid,
        export.gatewayGeneration, objectId, response.getTransferID(), response.getGeneration(),
        response.getOwnerToken(), principal);
    FileLeaseState state = new FileLeaseState(objectId, response.getTransferID(), response.getGeneration(),
        response.getOwnerToken(), principal, System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(remainingMillis),
        response.getContentPublished(), recoveredRanges);
    fileLeases.put(key, state);
    return response;
  }

  void refreshStagedRanges(ByteString exportUuid, long objectId, String principal) throws IOException {
    FileLeaseState fileLease = fileLease(exportUuid, objectId, principal);
    LeaseState export = leases.get(exportUuid);
    if (export == null || !isActive(exportUuid)) {
      throw new IOException("NFS export lease is not active");
    }
    ConcurrentSkipListMap<Long, NfsStagingStore.StagedRange> ranges = lookupStagedRanges(exportUuid,
        export.gatewayGeneration, objectId, fileLease.transferId, fileLease.generation, fileLease.ownerToken,
        principal);
    synchronized (fileLease) {
      fileLease.ranges.clear();
      fileLease.ranges.putAll(ranges);
    }
  }

  private ConcurrentSkipListMap<Long, NfsStagingStore.StagedRange> lookupStagedRanges(ByteString exportUuid,
      long gatewayGeneration, long objectId, String transferId, long leaseGeneration, ByteString ownerToken,
      String principal) throws IOException {
    ConcurrentSkipListMap<Long, NfsStagingStore.StagedRange> ranges = new ConcurrentSkipListMap<>();
    String marker = "";
    while (true) {
      org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.LookupNfsTransferRangesResponse page =
          readers.get(exportUuid).lookupTransferRanges(gatewayId, gatewayGeneration, objectId, transferId,
              leaseGeneration, ownerToken, principal, marker);
      for (org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.NfsTransferRangeEntry range :
          page.getRangesList()) {
        if (range.getTransferID().equals(transferId) && range.getTransactionIndex() > 0
            && range.getLength() > 0 && range.getLength() <= NfsObjectReader.MAX_READ_BYTES
            && range.getOffset() <= Long.MAX_VALUE - range.getLength()) {
          ranges.put(range.getTransactionIndex(), new NfsStagingStore.StagedRange(range.getStagingKey(),
              range.getOffset(), range.getLength(), range.getSha256()));
        } else {
          throw new IOException("OM returned an invalid NFS transfer range manifest");
        }
      }
      if (page.getEof()) {
        return ranges;
      }
      if (!page.hasNextMarker() || page.getNextMarker().isEmpty() || page.getNextMarker().equals(marker)) {
        throw new IOException("OM returned an invalid NFS transfer range page marker");
      }
      marker = page.getNextMarker();
    }
  }

  NfsFileCommitRequest fileCommitRequest(ByteString exportUuid, long objectId, String principal) throws IOException {
    NfsFileLeaseResponse fileLease = recoverFileLease(exportUuid, objectId, principal);
    LeaseState exportLease = leases.get(exportUuid);
    if (exportLease == null || !isActive(exportUuid)) {
      throw new IOException("NFS export lease is not active");
    }
    return NfsFileCommitRequest.newBuilder().setExportUUID(toUuid(exportUuid)).setGatewayID(gatewayId)
        .setGatewayGeneration(exportLease.gatewayGeneration).setTransferID(fileLease.getTransferID())
        .setFileGeneration(fileLease.getGeneration()).setOwnerToken(fileLease.getOwnerToken()).build();
  }

  long stagedFileSize(ByteString exportUuid, long objectId, String principal) throws IOException {
    FileLeaseState fileLease = fileLease(exportUuid, objectId, principal);
    if (fileLease.contentPublished) {
      return 0;
    }
    long size = 0;
    for (NfsStagingStore.StagedRange range : fileLease.ranges.values()) {
      size = Math.max(size, range.getOffset() + range.getLength());
    }
    return size;
  }

  void releaseFileLease(ByteString exportUuid, String transferId) throws IOException {
    FileLeaseKey key = new FileLeaseKey(exportUuid, transferId);
    FileLeaseState fileLease = fileLeases.get(key);
    LeaseState export = leases.get(exportUuid);
    if (fileLease != null && export != null && isActive(exportUuid)) {
      readers.get(exportUuid).updateFileLease(NfsFileLeaseRequest.Operation.RELEASE, gatewayId,
          export.gatewayGeneration, fileLease.objectId, fileLease.transferId, fileLease.generation,
          fileLease.ownerToken, 0);
      fileLeases.remove(key, fileLease);
    }
  }

  void markContentPublished(ByteString exportUuid, String transferId) throws IOException {
    FileLeaseState fileLease = fileLeases.get(new FileLeaseKey(exportUuid, transferId));
    if (fileLease == null || !isFileLeaseActive(exportUuid, transferId)) {
      throw new IOException("NFS file lease is not active");
    }
    fileLease.contentPublished = true;
  }

  void removeStagedRanges(ByteString exportUuid, String transferId) throws IOException {
    FileLeaseState fileLease = fileLeases.get(new FileLeaseKey(exportUuid, transferId));
    if (fileLease == null || !isFileLeaseActive(exportUuid, transferId)) {
      throw new IOException("NFS file lease is not active");
    }
    for (Map.Entry<Long, NfsStagingStore.StagedRange> entry : fileLease.ranges.entrySet()) {
      NfsStagingStore.StagedRange range = entry.getValue();
      updateTransferRange(exportUuid, transferId, NfsTransferRangeRequest.Operation.DELETE,
          range.getOffset(), range.getLength(), range.getQualifiedKey(), range.getSha256());
      readers.get(exportUuid).deleteStagedRange(range.getQualifiedKey());
      fileLease.ranges.remove(entry.getKey(), range);
    }
  }

  NfsTransferRangeResponse updateTransferRange(ByteString exportUuid, String transferId,
      NfsTransferRangeRequest.Operation operation, long offset, long length, String stagingKey,
      ByteString sha256) throws IOException {
    LeaseState export = leases.get(exportUuid);
    FileLeaseState fileLease = fileLeases.get(new FileLeaseKey(exportUuid, transferId));
    if (export == null || fileLease == null || !isActive(exportUuid)
        || !isFileLeaseActive(exportUuid, transferId)) {
      throw new IOException("NFS export or file lease is not active");
    }
    return readers.get(exportUuid).updateTransferRange(operation, gatewayId, export.gatewayGeneration,
        fileLease.objectId, transferId, fileLease.generation, fileLease.ownerToken, offset, length,
        stagingKey, sha256);
  }

  NfsTransferRangeResponse stageRange(ByteString exportUuid, String transferId, long objectId, String principal,
      long offset, byte[] data) throws IOException {
    FileLeaseState fileLease = fileLeases.get(new FileLeaseKey(exportUuid, transferId));
    if (fileLease == null || fileLease.objectId != objectId || !fileLease.principal.equals(principal)
        || !isFileLeaseActive(exportUuid, transferId)) {
      throw new IOException("NFS export or file lease is not active");
    }
    NfsStagingStore.StagedRange range = readers.get(exportUuid).stageRangeObject(transferId, offset, data);
    NfsTransferRangeResponse response = updateTransferRange(exportUuid, transferId,
        NfsTransferRangeRequest.Operation.UPSERT, offset, range.getLength(), range.getQualifiedKey(),
        range.getSha256());
    if (!response.hasTransactionIndex() || response.getTransactionIndex() <= 0) {
      throw new IOException("OM did not return the NFS range transaction index");
    }
    fileLease.ranges.put(response.getTransactionIndex(), range);
    fileLease.contentPublished = false;
    return response;
  }

  StagedRead readStaged(ByteString exportUuid, long objectId, String principal, long offset, int count)
      throws IOException {
    FileLeaseState fileLease = findFileLease(exportUuid, objectId, principal);
    if (fileLease == null) {
      return null;
    }
    if (fileLease.contentPublished) {
      return null;
    }
    if (offset < 0 || Integer.compareUnsigned(count, NfsObjectReader.MAX_READ_BYTES) > 0) {
      throw new IOException("NFS read range is outside the backend limit");
    }
    long fileSize = 0;
    for (NfsStagingStore.StagedRange range : fileLease.ranges.values()) {
      fileSize = Math.max(fileSize, range.getOffset() + range.getLength());
    }
    if (offset >= fileSize) {
      return new StagedRead(ByteString.EMPTY, true);
    }
    int length = (int) Math.min(Integer.toUnsignedLong(count), fileSize - offset);
    byte[] result = new byte[length];
    long end = offset + length;
    for (NfsStagingStore.StagedRange range : fileLease.ranges.values()) {
      long rangeOffset = range.getOffset();
      long rangeEnd = rangeOffset + range.getLength();
      long copyStart = Math.max(offset, rangeOffset);
      long copyEnd = Math.min(end, rangeEnd);
      if (copyStart < copyEnd) {
        byte[] data = readers.get(exportUuid).readStagedRange(range.getQualifiedKey());
        if (data.length != range.getLength()) {
          throw new IOException("NFS staged range length does not match its manifest");
        }
        if (!range.getSha256().equals(sha256(data))) {
          throw new IOException("NFS staged range checksum does not match its manifest");
        }
        System.arraycopy(data, (int) (copyStart - rangeOffset), result, (int) (copyStart - offset),
            (int) (copyEnd - copyStart));
      }
    }
    return new StagedRead(ByteString.copyFrom(result), end == fileSize);
  }

  private FileLeaseState fileLease(ByteString exportUuid, long objectId, String principal) throws IOException {
    FileLeaseState fileLease = findFileLease(exportUuid, objectId, principal);
    if (fileLease == null) {
      throw new IOException("NFS file lease is not active for the caller");
    }
    return fileLease;
  }

  private FileLeaseState findFileLease(ByteString exportUuid, long objectId, String principal) throws IOException {
    Map.Entry<FileLeaseKey, FileLeaseState> matched = fileLeases.entrySet().stream()
        .filter(entry -> entry.getKey().exportUuid.equals(exportUuid) && entry.getValue().objectId == objectId)
        .findFirst().orElse(null);
    if (matched == null) {
      return null;
    }
    FileLeaseState fileLease = matched.getValue();
    if (!fileLease.principal.equals(principal)) {
      return null;
    }
    if (!isFileLeaseActive(exportUuid, fileLease.transferId)) {
      fileLeases.remove(matched.getKey(), fileLease);
      return null;
    }
    return fileLease;
  }

  private static String toUuid(ByteString exportUuid) {
    java.nio.ByteBuffer bytes = exportUuid.asReadOnlyByteBuffer();
    return new java.util.UUID(bytes.getLong(), bytes.getLong()).toString();
  }

  private synchronized void renewAll() {
    for (Map.Entry<ByteString, NfsObjectReader> entry : readers.entrySet()) {
      LeaseState current = leases.get(entry.getKey());
      if (current == null) {
        continue;
      }
      try {
        NfsExportLeaseResponse response = entry.getValue().updateExportLease(Operation.RENEW, gatewayId,
            leaseDurationMillis, current.gatewayGeneration, current.generation);
        leases.replace(entry.getKey(), current, state(response));
      } catch (IOException | RuntimeException ex) {
        leases.remove(entry.getKey(), current);
        LOG.error("Lost NFS export lease for gateway {} and export {}", gatewayId, entry.getKey(), ex);
      }
    }
    for (Map.Entry<FileLeaseKey, FileLeaseState> entry : fileLeases.entrySet()) {
      renewFileLease(entry.getKey(), entry.getValue());
    }
  }

  private static ByteString sha256(byte[] data) throws IOException {
    try {
      return ByteString.copyFrom(MessageDigest.getInstance("SHA-256").digest(data));
    } catch (NoSuchAlgorithmException e) {
      throw new IOException("SHA-256 is unavailable", e);
    }
  }

  private void renewFileLease(FileLeaseKey key, FileLeaseState current) {
    LeaseState export = leases.get(key.exportUuid);
    if (export == null || !isActive(key.exportUuid)) {
      fileLeases.remove(key, current);
      return;
    }
    try {
      long fileLeaseDurationMillis = Math.max(1, leaseDurationMillis * 2 / 3);
      readers.get(key.exportUuid).updateFileLease(
          NfsFileLeaseRequest.Operation.RENEW, gatewayId, export.gatewayGeneration, current.objectId,
          current.transferId, current.generation, current.ownerToken, fileLeaseDurationMillis);
      current.expiresAtNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(fileLeaseDurationMillis);
    } catch (IOException | RuntimeException ex) {
      fileLeases.remove(key, current);
      LOG.error("Lost NFS file lease for gateway {} and transfer {}", gatewayId, current.transferId, ex);
    }
  }

  private LeaseState state(NfsExportLeaseResponse response) {
    long expiresAtNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(leaseDurationMillis);
    return new LeaseState(response.getGatewayGeneration(), response.getGeneration(), expiresAtNanos);
  }

  private void release(ByteString exportUuid, LeaseState state) {
    NfsObjectReader reader = Objects.requireNonNull(readers.get(exportUuid));
    try {
      reader.updateExportLease(Operation.RELEASE, gatewayId, 0, state.gatewayGeneration, state.generation);
    } catch (IOException | RuntimeException ex) {
      LOG.warn("Could not release NFS export lease for gateway {} and export {}", gatewayId, exportUuid, ex);
    }
  }

  @Override
  public void close() {
    renewer.shutdownNow();
    try {
      if (!renewer.awaitTermination(5, TimeUnit.SECONDS)) {
        LOG.warn("NFS export lease renewer did not stop; leases will expire without release");
        return;
      }
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      LOG.warn("Interrupted while stopping NFS export lease renewer; leases will expire without release", ex);
      return;
    }
    for (Map.Entry<ByteString, LeaseState> entry : leases.entrySet()) {
      if (leases.remove(entry.getKey(), entry.getValue())) {
        release(entry.getKey(), entry.getValue());
      }
    }
    fileLeases.clear();
  }

  private static final class LeaseState {
    private final long gatewayGeneration;
    private final long generation;
    private final long expiresAtNanos;

    private LeaseState(long gatewayGeneration, long generation, long expiresAtNanos) {
      this.gatewayGeneration = gatewayGeneration;
      this.generation = generation;
      this.expiresAtNanos = expiresAtNanos;
    }
  }

  private static final class FileLeaseKey {
    private final ByteString exportUuid;
    private final String transferId;

    private FileLeaseKey(ByteString exportUuid, String transferId) {
      this.exportUuid = exportUuid;
      this.transferId = transferId;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof FileLeaseKey)) {
        return false;
      }
      FileLeaseKey that = (FileLeaseKey) other;
      return exportUuid.equals(that.exportUuid) && transferId.equals(that.transferId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(exportUuid, transferId);
    }
  }

  private static final class FileLeaseState {
    private final long objectId;
    private final String transferId;
    private final long generation;
    private final ByteString ownerToken;
    private final String principal;
    private volatile long expiresAtNanos;
    private final ConcurrentMap<Long, NfsStagingStore.StagedRange> ranges;
    private volatile boolean contentPublished;

    @SuppressWarnings("checkstyle:ParameterNumber")
    private FileLeaseState(long objectId, String transferId, long generation, ByteString ownerToken,
        String principal, long expiresAtNanos, boolean contentPublished,
        ConcurrentMap<Long, NfsStagingStore.StagedRange> ranges) {
      this.objectId = objectId;
      this.transferId = transferId;
      this.generation = generation;
      this.ownerToken = ownerToken;
      this.principal = principal;
      this.expiresAtNanos = expiresAtNanos;
      this.contentPublished = contentPublished;
      this.ranges = ranges;
    }
  }

  static final class StagedRead {
    private final ByteString data;
    private final boolean eof;

    private StagedRead(ByteString data, boolean eof) {
      this.data = data;
      this.eof = eof;
    }

    ByteString getData() {
      return data;
    }

    boolean isEof() {
      return eof;
    }
  }
}
