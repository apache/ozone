package org.apache.hadoop.ozone.s3.endpoint;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(name = "Owner")
public class S3Owner {

  public static final S3Owner
      NOT_SUPPORTED_OWNER = new S3Owner("NOT-SUPPORTED", "Not Supported");

  @XmlElement(name = "DisplayName")
  private String displayName;

  @XmlElement(name = "ID")
  private String id;

  public S3Owner() {

  }

  public S3Owner(String id, String displayName) {
    this.id = id;
    this.displayName = displayName;
  }

  public String getDisplayName() {
    return displayName;
  }

  public void setDisplayName(String name) {
    this.displayName = name;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  @Override
  public String toString() {
    return "S3Owner{" +
        "displayName='" + displayName + '\'' +
        ", id='" + id + '\'' +
        '}';
  }
}
