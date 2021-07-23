package org.apache.hadoop.ozone.recon.api.types;

/**
 * Enum class for a path request's status.
 */
public enum PathStatus {
  OK, // Path exist
  PATH_NOT_FOUND, // Path not found
  TYPE_NOT_APPLICABLE // Path exists, but namespace is not applicable to request
}
