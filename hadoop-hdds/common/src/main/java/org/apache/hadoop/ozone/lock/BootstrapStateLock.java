package org.apache.hadoop.ozone.lock;

public interface BootstrapStateLock {
  public void lockBootstrapState() throws InterruptedException;
  public void unlockBootstrapState();
}
