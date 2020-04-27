package org.apache.hadoop.hdds.conf;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Methods annotated with this annotation will be called after object creation.
 */
@Retention(RUNTIME)
@Target(METHOD)
public @interface PostConstruct {
}