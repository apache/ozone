package org.apache.hadoop.hdds.upgrade;

import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to mark a class that belongs to a specific HDDS Layout Version.
 */
@Target({ElementType.TYPE, ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface BelongsToHDDSLayoutVersion {
    HDDSLayoutFeature value();
}
