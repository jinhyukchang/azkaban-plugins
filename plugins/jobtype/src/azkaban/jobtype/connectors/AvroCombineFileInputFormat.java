package azkaban.jobtype.connectors;

import java.io.IOException;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroRecordReader;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.CombineFileInputFormat;
import org.apache.hadoop.mapred.lib.CombineFileRecordReader;
import org.apache.hadoop.mapred.lib.CombineFileSplit;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


/**
 * Combining small files for Avro input file format. This class can be also used to control number of mappers.
 */
public class AvroCombineFileInputFormat extends CombineFileInputFormat<AvroWrapper<GenericRecord>, NullWritable> {
  @Override
  public RecordReader<AvroWrapper<GenericRecord>, NullWritable> getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {
    return new CombineFileRecordReader(job,(CombineFileSplit)split,reporter, MyCombineFileRecordReader.class);
  }

  @Override
  protected boolean isSplitable(FileSystem fs, Path filename) {
    return false;
  }

  public static class MyCombineFileRecordReader implements RecordReader<AvroWrapper<GenericRecord>, NullWritable> {

    private final AvroRecordReader<GenericRecord> delegate;

    public MyCombineFileRecordReader(CombineFileSplit split, Configuration conf, Reporter reporter, Integer index) throws IOException {
        FileSplit filesplit = new FileSplit(split.getPath(index), split.getOffset(index), split.getLength(index), split.getLocations());
        delegate = new AvroRecordReader<GenericRecord>(new JobConf(conf), filesplit);
    }

    @Override
    public void close() throws IOException {
      delegate.close();
    }

    @Override
    public AvroWrapper<GenericRecord> createKey() {
      return delegate.createKey();
    }

    @Override
    public NullWritable createValue() {
      return delegate.createValue();
    }

    @Override
    public long getPos() throws IOException {
      return delegate.getPos();
    }

    @Override
    public float getProgress() throws IOException {
      return delegate.getProgress();
    }

    @Override
    public boolean next(AvroWrapper<GenericRecord> key, NullWritable val) throws IOException {
      return delegate.next(key, val);
    }
  }
}