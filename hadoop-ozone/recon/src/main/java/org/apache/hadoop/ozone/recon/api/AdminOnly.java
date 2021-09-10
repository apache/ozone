package org.apache.hadoop.ozone.recon.api;

import javax.ws.rs.Path;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to apply to endpoint classes that also have a {@link Path}
 * annotation that will cause their access to be restricted to ozone and
 * recon administrators only.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface AdminOnly {
}
