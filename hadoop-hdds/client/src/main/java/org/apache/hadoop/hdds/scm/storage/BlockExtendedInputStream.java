package org.apache.hadoop.hdds.scm.storage;

import org.apache.hadoop.hdds.client.BlockID;

public abstract class BlockExtendedInputStream  extends ExtendedInputStream {

  public abstract BlockID getBlockID();

  public abstract long getRemaining();

  public abstract long getLength();

}
