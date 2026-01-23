/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.s3.endpoint;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import org.apache.hadoop.ozone.s3.util.S3Consts;

/**
 * S3 tagging.
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(name = "Tagging",
    namespace = S3Consts.S3_XML_NAMESPACE)
public class S3Tagging {

  @XmlElement(name = "TagSet")
  private TagSet tagSet;

  public S3Tagging() {

  }

  public S3Tagging(TagSet tagSet) {
    this.tagSet = tagSet;
  }

  public TagSet getTagSet() {
    return tagSet;
  }

  public void setTagSet(TagSet tagSet) {
    this.tagSet = tagSet;
  }

  /**
   * Entity for child element TagSet.
   */
  @XmlAccessorType(XmlAccessType.FIELD)
  @XmlRootElement(name = "TagSet")
  public static class TagSet {
    @XmlElement(name = "Tag")
    private List<Tag> tags = new ArrayList<>();

    public TagSet() {
    }

    public TagSet(List<Tag> tags) {
      this.tags = tags;
    }

    public List<Tag> getTags() {
      return tags;
    }

    public void setTags(List<Tag> tags) {
      this.tags = tags;
    }
  }

  /**
   * Entity for child element Tag.
   */
  @XmlAccessorType(XmlAccessType.FIELD)
  @XmlRootElement(name = "Tag")
  public static class Tag {
    @XmlElement(name = "Key")
    private String key;

    @XmlElement(name = "Value")
    private String value;

    public Tag() {
    }

    public Tag(String key, String value) {
      this.key = key;
      this.value = value;
    }

    public String getKey() {
      return key;
    }

    public void setKey(String key) {
      this.key = key;
    }

    public String getValue() {
      return value;
    }

    public void setValue(String value) {
      this.value = value;
    }
  }

  /**
   * Creates a S3 tagging instance (xml representation) from a Map retrieved
   * from OM.
   * @param tagMap Map representing the tags.
   * @return {@link S3Tagging}
   */
  public static S3Tagging fromMap(Map<String, String> tagMap) {
    List<Tag> tags = tagMap.entrySet()
        .stream()
        .map(
            tagEntry -> new Tag(tagEntry.getKey(), tagEntry.getValue())
        )
        .collect(Collectors.toList());
    return new S3Tagging(new TagSet(tags));
  }

  /**
   * Additional XML validation logic for S3 tagging.
   */
  public void validate() {
    if (tagSet == null) {
      throw new IllegalArgumentException("TagSet needs to be specified");
    }

    if (tagSet.getTags().isEmpty()) {
      throw new IllegalArgumentException("Tags need to be specified and cannot be empty");
    }

    for (Tag tag: tagSet.getTags()) {
      if (tag.getKey() == null) {
        throw new IllegalArgumentException("Some tag keys are not specified");
      }
      if (tag.getValue() == null) {
        throw new IllegalArgumentException("Tag value for tag " + tag.getKey() + " is not specified");
      }
    }
  }
}
