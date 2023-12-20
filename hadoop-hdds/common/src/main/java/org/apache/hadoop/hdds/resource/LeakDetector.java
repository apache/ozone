package org.apache.hadoop.hdds.resource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;

/**
 * Simple general resource leak detector using {@link ReferenceQueue} to observe resource object life-cycle
 * and perform assertions to verify proper resource closure after they are GCed.
 *
 * <pre> {@code
 * class MyResource implements Leakable {
 *   public MyResource() {
 *     ResourceLeakDetector.LEAK_DETECTOR.watch(this);
 *   }
 *   public void check() {
 *     // assertions to verify this resource closure.
 *   }
 * }
 * class ResourceLeakDetector {
 *    public final LeakDetector LEAK_DETECTOR = new LeakDetector("MyResource");
 * }
 * }</pre>
 * @see Leakable
 */
public class LeakDetector implements Runnable {
  public static final Logger LOG = LoggerFactory.getLogger(LeakDetector.class);
  private final ReferenceQueue<Leakable> queue = new ReferenceQueue<>();
  private final String name;

  public LeakDetector(String name) {
    this.name = name;
    start();
  }

  private void start() {
    Thread t = new Thread(this);
    t.setName(LeakDetector.class.getSimpleName() + "-" + name);
    t.setDaemon(true);
    t.start();
  }

  @Override
  public void run() {
    while (true) {
      try {
        WeakReference<Leakable> ref = (WeakReference<Leakable>) queue.remove();
        Leakable leakable = ref.get();
        if (leakable != null) {
          leakable.check();
        }
      } catch (InterruptedException e) {
        LOG.warn("Thread interrupted, exiting.");
        break;
      }
    }

    LOG.warn("Existing leak detector {}.", name);
  }

  public void watch(Leakable leakable) {
    // Intentionally ignored.
    WeakReference ignored = new WeakReference(leakable, queue);
  }
}
