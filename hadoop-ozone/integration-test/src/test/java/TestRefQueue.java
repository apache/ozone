import org.jooq.meta.derby.sys.Sys;

import java.io.IOException;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

public class TestRefQueue {
  static ReferenceQueue<MyObject> MY_QUEUE = new ReferenceQueue<>();
  static Set<WeakReference<MyObject>> refs = Collections.newSetFromMap(new IdentityHashMap<>());

  public static void main(String[] args) throws IOException {
    Thread t = new Thread(() -> {
      while (true) {
        MyReference r = null;
        try {
          r = (MyReference) MY_QUEUE.remove();
          refs.remove(r);
        } catch (InterruptedException e) {
          break;
        }
        System.out.println(r.name + " eligible for collection");
      }
      System.out.println("Escaping checker");
    });
    t.setDaemon(true);
    t.start();

    new MyObject();
    new MyObject();
    new MyObject();

    System.gc();
    System.in.read();

  }

  static class MyObject {
    MyObject() {
      //normal init...
      MyReference ref = new MyReference(this, MY_QUEUE);
      refs.add(ref);
    }

    @Override
    protected void finalize() throws Throwable {
      System.out.println("Finalize object");
    }
  }
  static class MyReference extends WeakReference<MyObject> {
    public final String name;

    public MyReference(MyObject o, ReferenceQueue<MyObject> q) {
      super(o, q);
      name = o.toString();
    }
  }


}
