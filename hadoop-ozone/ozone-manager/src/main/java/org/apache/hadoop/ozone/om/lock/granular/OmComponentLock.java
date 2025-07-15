package org.apache.hadoop.ozone.om.lock.granular;

import org.apache.ratis.util.Preconditions;
import java.util.Comparator;
import java.util.Objects;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

/** Locks for components volume, bucket and keys. */
public class OmComponentLock {
  private static final Comparator<OmComponentLock> COMPARATOR
      = Comparator.comparing(OmComponentLock::getComponent)
      .thenComparingInt(OmComponentLock::getStripeIndex);

  public static Comparator<OmComponentLock> getComparator() {
    return COMPARATOR;
  }

  public enum Component {
    VOLUME, BUCKET, KEY
  }

  public enum Type {
    READ, WRITE
  }

  private final String name;
  private final Component component;
  private final Type type;
  private final int stripeIndex;
  private final Lock stripeLock;
  private boolean locked = false;

  OmComponentLock(String name, Component component, Type type, int stripeIndex, ReadWriteLock stripeLock) {
    Objects.requireNonNull(stripeLock, "stripeLock == null");
    this.name = Objects.requireNonNull(name, "name == null");
    this.component = Objects.requireNonNull(component, "component == null");
    this.type = Objects.requireNonNull(type, "type == null");
    this.stripeIndex = stripeIndex;
    this.stripeLock = type == Type.READ ? stripeLock.readLock() : stripeLock.writeLock();
  }

  Component getComponent() {
    return component;
  }

  int getStripeIndex() {
    return stripeIndex;
  }

  void acquire() {
    Preconditions.assertTrue(!locked, () -> this + " is already acquired");
    stripeLock.lock();
    locked = true;
  }

  void release() {
    Preconditions.assertTrue(locked, () -> this + " is NOT yet acquired");
    locked = false;
    stripeLock.unlock();
  }


  @Override
  public String toString() {
    return name + ":" + component + "_" + type + ":s" + stripeIndex + "-" + (locked ? "LOCKED" : "unlocked");
  }
}
