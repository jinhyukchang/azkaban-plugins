package azkaban.jobtype.connectors;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.io.Writable;

import azkaban.jobtype.javautils.ValidationUtils;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.linkedin.espresso.pub.ContentType;

public class EspressoEntry implements Writable {

  private AvroWrapper<GenericRecord> src;
  private String key;
  private Optional<ImmutableList<String>> subKeys;
  private String content;
  private ContentType contentType;

  public EspressoEntry() {} //Default cstr for MR to instantiate it.

  public EspressoEntry(AvroWrapper<GenericRecord> src, String content, String key, ContentType contentType, ImmutableList<String> subKeys) {
    ValidationUtils.validateNotEmpty(content, "content");
    ValidationUtils.validateNotEmpty(key, "key");
    ValidationUtils.validateNotNull(contentType, "contentType");
    ValidationUtils.validateNotNull(src, "src");
    this.src = src;
    this.content = content;
    this.key = key;
    this.contentType = contentType;
    this.subKeys = Optional.fromNullable(subKeys);
  }

  public EspressoEntry(AvroWrapper<GenericRecord> src, String content, String primaryKey, ContentType contentType) {
    this(src, content, primaryKey, contentType, null);
  }

  public String getKey() {
    return key;
  }

  public Optional<ImmutableList<String>> getSubKeys() {
    return subKeys;
  }

  public String getContent() {
    return content;
  }

  public ContentType getContentType() {
    return contentType;
  }

  public AvroWrapper<GenericRecord> getSrc() {
    return src;
  }

  @Override
  public String toString() {
    return "EspressoEntry [src=" + src + ", key=" + key + ", subKeys=" + subKeys + ", content=" + content
        + ", contentType=" + contentType + "]";
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.key = in.readUTF();
    int listSize = in.readInt();
    if(listSize == 0) {
      subKeys = Optional.absent();
    } else {
      Builder<String> builder = ImmutableList.builder();
      for(int i = 0; i < listSize; i++) {
        builder.add(in.readUTF());
      }
      subKeys = Optional.of(builder.build());
    }
    this.content = in.readUTF();
    this.contentType = ContentType.valueOf(in.readUTF());
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(key);

    int count = 0;
    if(subKeys.isPresent()) {
      count = subKeys.get().size();
    }
    out.writeInt(count);
    if(count > 0) {
      for(String subKey : subKeys.get()) {
        out.writeUTF(subKey);
      }
    }

    out.writeUTF(content);
    out.writeUTF(contentType.name());
  }
}
