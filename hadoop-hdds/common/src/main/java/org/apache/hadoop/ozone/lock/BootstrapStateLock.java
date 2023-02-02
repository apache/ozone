package org.apache.hadoop.ozone.lock;

public interface BootstrapStateLock {
  public void lockBootstrapState();
  public void unlockBootstrapState();
}
