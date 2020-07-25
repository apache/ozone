package org.apache.hadoop.hdds.cli;

public interface SubcommandWithParent {
  Class<?> getParentType();
}
