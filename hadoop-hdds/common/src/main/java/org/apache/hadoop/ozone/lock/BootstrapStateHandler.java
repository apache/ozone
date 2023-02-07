package org.apache.hadoop.ozone.lock;

public interface BootstrapStateHandler {
  public void lockBootstrapState() throws InterruptedException;
  public void unlockBootstrapState();
}
