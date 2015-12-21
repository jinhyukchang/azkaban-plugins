package azkaban.jobtype.connectors;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.mapred.AvroInputFormat;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroOutputFormat;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.log4j.Logger;

import azkaban.jobtype.connectors.HdfsToEspressoSingleputMapper;
import azkaban.jobtype.javautils.AbstractHadoopJob;
import azkaban.jobtype.javautils.HadoopUtils;
import azkaban.utils.Props;

public class HdfsToEspressoHadoopJob extends AbstractHadoopJob {
  private static final Logger log = Logger.getLogger(HdfsToEspressoHadoopJob.class);

  private final JobConf _conf;
  private final Path _inputPath;

  public HdfsToEspressoHadoopJob(String name, Props props) throws IOException, URISyntaxException {
    super(name, props);
    _inputPath = new Path(props.getString("input.paths"));
    props.put("output.path", props.get("error.path"));
    props.put("force.output.overwrite", Boolean.toString(props.getBoolean("force.error.overwrite", false)));

    _conf = initializeConf();
  }

  private void initializeConfCommon(JobConf conf) throws IOException {
    FileInputFormat.addInputPath(conf, _inputPath);
    Path outputPath = new Path(getProps().getString("error.path"));
    FileOutputFormat.setOutputPath(conf, outputPath);

    AvroJob.setOutputSchema(conf, getAvroSchemaFromAvro(_inputPath.getFileSystem(conf), _inputPath));

    conf.set(HdfsToEspressoConstants.ESPRESSO_ENDPOINT_KEY, getProps().getString(HdfsToEspressoConstants.ESPRESSO_ENDPOINT_KEY));
    conf.set(HdfsToEspressoConstants.ESPRESSO_DB_KEY, getProps().getString(HdfsToEspressoConstants.ESPRESSO_DB_KEY));
    conf.set(HdfsToEspressoConstants.ESPRESSO_TABLE_KEY, getProps().getString(HdfsToEspressoConstants.ESPRESSO_TABLE_KEY));
    conf.set(HdfsToEspressoConstants.ESPRESSO_CONTENT_TYPE_KEY, getProps().getString(HdfsToEspressoConstants.ESPRESSO_CONTENT_TYPE_KEY));
    conf.set(HdfsToEspressoConstants.ESPRESSO_KEY, getProps().getString(HdfsToEspressoConstants.ESPRESSO_KEY));

    int qps = getProps().getInt(HdfsToEspressoConstants.ESPRESSO_PUT_QPS_KEY);
    if(qps > HdfsToEspressoConstants.ESPRESSO_MAX_PUT_QPS) {
      throw new IllegalArgumentException("QPS cannot exceed more than " + HdfsToEspressoConstants.ESPRESSO_MAX_PUT_QPS);
    }
    if(qps <= 0) {
      throw new IllegalArgumentException("QPS should be positive number");
    }
    log.info("Espresso put QPS will be up to: " + qps);

    int numThreads = getProps().getInt(HdfsToEspressoConstants.MR_NUM_THREAD_KEY, HdfsToEspressoConstants.MR_NODE_MAX_THREADS);
    if(numThreads > HdfsToEspressoConstants.MR_NODE_MAX_THREADS) {
      throw new IllegalArgumentException("Cannot exceed more than " + HdfsToEspressoConstants.MR_NODE_MAX_THREADS + " threads per node. "
                                         + "Please update " + HdfsToEspressoConstants.MR_NUM_THREAD_KEY);
    }

    if(numThreads < 1) {
      throw new IllegalArgumentException("Number of threads needs to be positive number " + numThreads);
    }

    conf.setInt(HdfsToEspressoConstants.MR_NUM_THREAD_KEY, numThreads);
    log.info("Number of threads per node: " + numThreads);

    conf.setInt(HdfsToEspressoConstants.ESPRESSO_PUT_QPS_KEY, qps);
    conf.set(JobConf.MAPRED_MAP_TASK_JAVA_OPTS, "-Xmx384m");
  }

  private JobConf initializeConfSingleput() throws IOException, URISyntaxException {
    JobConf conf = super.createJobConf(HdfsToEspressoSingleputMapper.class);
    initializeConfCommon(conf);
    if (getProps().containsKey(HdfsToEspressoConstants.ESPRESSO_SUBKEY)) {
      log.info("subkeys: " + getProps().getString(HdfsToEspressoConstants.ESPRESSO_SUBKEY));
      conf.set(HdfsToEspressoConstants.ESPRESSO_SUBKEY, getProps().getString(HdfsToEspressoConstants.ESPRESSO_SUBKEY));
    } else {
      log.info("No subkeys");
    }

    conf.setInputFormat(AvroInputFormat.class);
    conf.setOutputFormat(AvroOutputFormat.class);
    conf.setOutputKeyComparatorClass(Text.Comparator.class);
    conf.setMapOutputKeyClass(AvroWrapper.class);
    conf.setMapOutputValueClass(NullWritable.class);
    conf.setNumReduceTasks(0); //Reducer is not needed.

    int numMapper = getProps().getInt(HdfsToEspressoConstants.MR_NUM_NODES_KEY, HdfsToEspressoConstants.DEFAULT_NO_NODES);
    if(numMapper < 1) {
      throw new IllegalArgumentException("Number of nodes cannot be less than 1");
    }

    log.info("Desired number of mapper nodes: " + numMapper);
    long splitSize = HadoopUtils.computeSplitSize(_inputPath.getFileSystem(conf), _inputPath, numMapper);
    conf.setLong("mapreduce.input.fileinputformat.split.minsize", splitSize);
    log.debug("Set mapreduce.input.fileinputformat.split.minsize to " + Long.MAX_VALUE);

    conf.setLong("mapreduce.input.fileinputformat.split.maxsize", splitSize);
    log.debug("Set mapreduce.input.fileinputformat.split.maxsize to " + splitSize);
    conf.setInputFormat(AvroCombineFileInputFormat.class);
    conf.set(JobConf.MAPRED_MAP_TASK_JAVA_OPTS, "-Xmx384m");

    return conf;
  }

  private JobConf initializeConfMultiput() throws IOException, URISyntaxException {
    JobConf conf = super.createJobConf(HdfsToEspressoMultiputMapper.class, HdfsToEspressoMultiputReducer.class);
    initializeConfCommon(conf);

    Schema schema = getAvroSchemaFromAvro(_inputPath.getFileSystem(conf), _inputPath);
    AvroJob.setMapOutputSchema(conf, Pair.getPairSchema(schema, schema));

    conf.setInputFormat(AvroInputFormat.class);
    conf.setOutputFormat(AvroOutputFormat.class);
    conf.setOutputKeyComparatorClass(Text.Comparator.class);
    conf.setMapOutputKeyClass(Text.class);
    conf.setMapOutputValueClass(AvroWrapper.class);

    conf.set(HdfsToEspressoConstants.ESPRESSO_SUBKEY, getProps().getString(HdfsToEspressoConstants.ESPRESSO_SUBKEY));

    int numReducer = getProps().getInt(HdfsToEspressoConstants.MR_NUM_NODES_KEY, HdfsToEspressoConstants.DEFAULT_NO_NODES);
    if(numReducer < 1) {
      throw new IllegalArgumentException("Number of nodes cannot be less than 1");
    }
    log.info("Desired number of reducer nodes: " + numReducer);
    conf.setNumReduceTasks(numReducer);

    return conf;
  }

  private JobConf initializeConf() {
    try {
      boolean isMultiput = getProps().getBoolean(HdfsToEspressoConstants.ESPRESSO_IS_MULTIPUT_KEY, false);
      if(isMultiput) {
        log.info("Initializing JobConf for Espresso multiput. This will use mapper and reducer.");
        return initializeConfMultiput();
      }
      log.info("Initializing JobConf for Espresso single put. This will only use mapper.");
      return initializeConfSingleput();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private Schema getAvroSchemaFromAvro(FileSystem fs, Path path) throws IOException {
    if(!fs.exists(path)) {
      throw new IllegalArgumentException("Path " + path + " does not exist. Cannot extract Avro schema.");
    }
    if(fs.isFile(path)) {
      DataFileStream<Object> stream = null;
      try {
        stream = createAvroDataStream(fs, path);
        Schema schema = stream.getSchema();
        stream.close();
        return schema;
      } finally {
        if(stream != null) {
          stream.close();
        }
      }
    }

    //Path is directory
    Throwable t = null;
    for(FileStatus fileStatus : fs.listStatus(path)) {
      if(!fileStatus.isDir()) { //This method is not responsible for below 2nd level sub directories.
        if(!fileStatus.getPath().getName().endsWith(".avro")) {
          continue;
        }
        try {
          return getAvroSchemaFromAvro(fs, fileStatus.getPath());
        } catch (Exception e) {
          log.warn("Failed to get Schema from " + fileStatus.getPath(), e);
          t = e;
        }
      }
    }
    throw new RuntimeException("Failed to create Schema using path " + path, t);
  }

  private DataFileStream<Object> createAvroDataStream(FileSystem fs, Path path)
      throws IOException {
    if (log.isDebugEnabled()) {
      log.debug("path:" + path.toUri().getPath());
    }

    GenericDatumReader<Object> avroReader = new GenericDatumReader<Object>();
    InputStream hdfsInputStream = null;
    try {
      hdfsInputStream = fs.open(path);
    } catch (IOException e) {
      throw e;
    }

    DataFileStream<Object> avroDataFileStream = null;
    try {
      avroDataFileStream =
          new DataFileStream<Object>(hdfsInputStream, avroReader);
    } catch (IOException e) {
      if (hdfsInputStream != null) {
        hdfsInputStream.close();
      }
      throw e;
    }
    return avroDataFileStream;
  }

  private long computeSplitSize(FileSystem fs, Path inputPath, long concurrency) throws IOException {
    if(concurrency <= 0L) {
      throw new IllegalArgumentException("Concurrency level should be positive number");
    }
    long sum = 0L;
    for(FileStatus fileStatus : fs.listStatus(inputPath)) {
      if(!fileStatus.isDir()) {
        long bytes = fileStatus.getLen();
        if (Long.MAX_VALUE - bytes <= sum) { //Overflow case.
          sum = Long.MAX_VALUE;
          break;
        }
        sum += fileStatus.getLen();
      }
    }
    log.info("Total file size: " + sum + " bytes");
    return sum / concurrency;
  }

  @Override
  public void run() throws Exception {
    log.info(String.format("Starting %s", getClass().getSimpleName()));
    super.run();
  }

  @Override
  public JobConf getJobConf() {
    return _conf;
  }
}