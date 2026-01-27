package org.apache.hadoop.ozone.util;

import java.util.PriorityQueue;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * A cache that creates new objects up to the specified capacity and returns cached ones after.
 */
public class UsageBasedCache<T> {
  private final int capacity;
  private final Supplier<T> factory;
  private final PriorityQueue<InstanceWrapper<T>> queue;
  private final ReentrantLock lock = new ReentrantLock();

  public UsageBasedCache(int capacity, Supplier<T> factory) {
    this.capacity = capacity;
    this.factory = factory;
    this.queue = new PriorityQueue<>(capacity, (a, b) -> {
      int cmp = Integer.compare(a.getUsage(), b.getUsage());
      if (cmp != 0) {
        return cmp;
      } else {
        return Integer.compare(System.identityHashCode(a.instance), System.identityHashCode(b.instance));
      }
    });
  }

  public T get() {
    lock.lock();
    try {
      InstanceWrapper<T> wrapper;

      if (queue.size() < capacity) {
        wrapper = new InstanceWrapper<>(factory.get(), 1);
      } else {
        wrapper = queue.poll();
        wrapper.incrementUsage();
      }

      queue.offer(wrapper);
      return wrapper.instance;
    } finally {
      lock.unlock();
    }
  }

  public void forEach(Consumer<T> action) {
    lock.lock();
    try {
      for (InstanceWrapper<T> wrapper : queue) {
        action.accept(wrapper.instance);
      }
    } finally {
      lock.unlock();
    }
  }

  public void clear() {
    lock.lock();
    try {
      queue.clear();
    } finally {
      lock.unlock();
    }
  }

  private static class InstanceWrapper<T> {
    private final T instance;
    private int usage;

    InstanceWrapper(T instance, int usage) {
      this.instance = instance;
      this.usage = usage;
    }

    void incrementUsage() {
      this.usage++;
    }

    public int getUsage() {
      return usage;
    }
  }
}
