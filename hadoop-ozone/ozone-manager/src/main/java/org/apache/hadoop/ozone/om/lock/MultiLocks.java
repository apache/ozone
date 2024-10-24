package org.apache.hadoop.ozone.om.lock;

import org.apache.hadoop.ozone.om.exceptions.OMException;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Queue;

/**
 * Class to take multiple locks on a resource.
 */
public class MultiLocks<T> {
  private final Queue<T> objectLocks;
  private final IOzoneManagerLock lock;
  private final OzoneManagerLock.Resource resource;
  private final boolean writeLock;

  public MultiLocks(IOzoneManagerLock lock, OzoneManagerLock.Resource resource, boolean writeLock) {
    this.writeLock = writeLock;
    this.resource = resource;
    this.lock = lock;
    this.objectLocks = new LinkedList<>();
  }

  public OMLockDetails acquireLock(Collection<T> objects) throws OMException {
    if (!objectLocks.isEmpty()) {
      throw new OMException("More locks cannot be acquired when locks have been already acquired. Locks acquired : "
          + objectLocks, OMException.ResultCodes.INTERNAL_ERROR);
    }
    OMLockDetails omLockDetails = OMLockDetails.EMPTY_DETAILS_LOCK_ACQUIRED;
    for (T object : objects) {
      if (object != null) {
        omLockDetails = this.writeLock ? lock.acquireWriteLock(resource, object.toString())
            : lock.acquireReadLock(resource, object.toString());
        objectLocks.add(object);
        if (!omLockDetails.isLockAcquired()) {
          break;
        }
      }
    }
    if (!omLockDetails.isLockAcquired()) {
      releaseLock();
    }
    return omLockDetails;
  }

  public void releaseLock() {
    while (!objectLocks.isEmpty()) {
      T object = objectLocks.poll();
      OMLockDetails lockDetails = this.writeLock ? lock.releaseWriteLock(resource, object.toString())
          : lock.releaseReadLock(resource, object.toString());
    }
  }
}
