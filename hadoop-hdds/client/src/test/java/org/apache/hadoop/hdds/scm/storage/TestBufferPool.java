package org.apache.hadoop.hdds.scm.storage;

import org.apache.hadoop.ozone.common.ChunkBuffer;

import org.junit.Assert;
import org.junit.Test;

public class TestBufferPool {

  @Test
  public void releaseAndReallocate() {
    BufferPool pool = new BufferPool(1024, 8);
    ChunkBuffer cb1 = pool.allocateBuffer(0);
    ChunkBuffer cb2 = pool.allocateBuffer(0);
    ChunkBuffer cb3 = pool.allocateBuffer(0);

    pool.releaseBuffer(cb1);

    //current state cb2, -> cb3, cb1
    final ChunkBuffer allocated = pool.allocateBuffer(0);
    Assert.assertEquals(3, pool.getSize());
    Assert.assertEquals(cb1, allocated);
  }

}