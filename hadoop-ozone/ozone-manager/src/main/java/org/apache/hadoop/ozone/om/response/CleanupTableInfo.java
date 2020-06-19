package org.apache.hadoop.ozone.om.response;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Inherited
@InterfaceStability.Evolving
public @interface CleanupTableInfo {

  /**
   * Array of tables affected by this operation. This information will be used
   * during cleanup table cache.
   * @return list of table names.
   */
  String[] cleanupTables();
}
