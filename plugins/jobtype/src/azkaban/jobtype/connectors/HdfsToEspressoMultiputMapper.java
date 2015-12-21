package azkaban.jobtype.connectors;

import java.io.IOException;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

import azkaban.jobtype.javautils.HadoopUtils;
import azkaban.jobtype.javautils.ValidationUtils;

import com.google.common.collect.ImmutableList;

public class HdfsToEspressoMultiputMapper extends MapReduceBase implements Mapper<AvroWrapper<GenericRecord>, NullWritable, Text, AvroWrapper<GenericRecord>> {
  private static final Logger log = Logger.getLogger(HdfsToEspressoMultiputMapper.class);
  private String keyColName;
  private ImmutableList<String> subKeyColNames;
  static {
    HadoopUtils.setLoggerOnNodes();
  }

  @Override
  public void configure(JobConf job) {
    keyColName = job.get(HdfsToEspressoConstants.ESPRESSO_KEY);
    ValidationUtils.validateNotEmpty(keyColName, HdfsToEspressoConstants.ESPRESSO_KEY);
    subKeyColNames = parseSubKeysString(job.get(HdfsToEspressoConstants.ESPRESSO_SUBKEY));
    ValidationUtils.validateNotEmpty(subKeyColNames, HdfsToEspressoConstants.ESPRESSO_SUBKEY);
  }

  private ImmutableList<String> parseSubKeysString(String subKeysStr) {
    if(StringUtils.isEmpty(subKeysStr)) {
      return null;
    }
    String[] subKeysArr = subKeysStr.split(",");
    if(subKeysArr.length == 0) {
      return null;
    }
    return ImmutableList.<String>builder().add(subKeysArr).build();
  }

  @Override
  public void map(AvroWrapper<GenericRecord> avroWrapper, NullWritable val, OutputCollector<Text, AvroWrapper<GenericRecord>> output, Reporter reporter)
      throws IOException {
    GenericRecord genericRecord = avroWrapper.datum();
    String entryKey = String.valueOf(genericRecord.get(keyColName));
    if (StringUtils.isEmpty(entryKey)) {
      log.warn("Record does not have primary key. Skipping: " + genericRecord);
      return;
    }

    ImmutableList.Builder<String> listBuilder = ImmutableList.builder();
    for(String colName : subKeyColNames) {
      listBuilder.add(String.valueOf(genericRecord.get(colName)));
    }
    output.collect(new Text(entryKey), avroWrapper);
  }

  @Override
  public void close() {
    log.info("Mapper finished processing.");

  }
}
