package org.apache.hadoop.ozone.freon.containergenerator;

import java.util.concurrent.Callable;

import org.apache.hadoop.ozone.freon.BaseFreonGenerator;

import picocli.CommandLine.Option;

public abstract class BaseGenerator extends BaseFreonGenerator implements
    Callable<Void> {

  @Option(names = {"-u", "--user"},
      description = "Owner of the files",
      defaultValue = "ozone")
  private static String userId;

  @Option(names = {"--key-size"},
      description = "Size of the generated keys (in bytes) in each of the "
          + "containers",
      defaultValue = "16000000")
  private int keySize;

  @Option(names = {"--size"},
      description = "Size of generated containers",
      defaultValue = "5000000000")
  private long containerSize;

  @Option(names = {"--from"},
      description = "First container index to use",
      defaultValue = "1")
  private long containerIdOffset;

  public static String getUserId() {
    return userId;
  }

  public int getKeysPerContainer() {
    return (int) (getContainerSize() / getKeySize());
  }

  public long getContainerIdOffset() {
    return containerIdOffset;
  }

  public long getContainerSize() {
    return containerSize;
  }

  public int getKeySize() {
    return keySize;
  }
}
