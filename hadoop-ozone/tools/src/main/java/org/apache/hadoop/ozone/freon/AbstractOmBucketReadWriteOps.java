package org.apache.hadoop.ozone.freon;

import java.util.concurrent.Callable;

/**
 * Abstract class for OmBucketReadWriteFileOps/KeyOps Freon class
 * implementations.
 */
public abstract class AbstractOmBucketReadWriteOps extends BaseFreonGenerator
    implements Callable<Void> {

  /**
   * Generates a synthetic read file/key operations workload.
   *
   * @return total files/keys read
   * @throws Exception
   */
  public abstract int readOperations() throws Exception;

  /**
   * Generates a synthetic write file/key operations workload.
   *
   * @return total files/keys written
   * @throws Exception
   */
  public abstract int writeOperations() throws Exception;

  /**
   * Creates a file/key under the given directory/path.
   *
   * @param path
   * @throws Exception
   */
  public abstract void create(String path) throws Exception;
}
