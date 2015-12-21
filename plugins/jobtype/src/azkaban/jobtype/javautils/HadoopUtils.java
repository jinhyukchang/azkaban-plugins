/*
 * Copyright 2012 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package azkaban.jobtype.javautils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Enumeration;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import azkaban.utils.Props;

public class HadoopUtils {

  private static final Logger logger = Logger.getLogger(HadoopUtils.class);

  public static void setClassLoaderAndJar(JobConf conf, Class<?> jobClass) {
    conf.setClassLoader(Thread.currentThread().getContextClassLoader());
    String jar =
        findContainingJar(jobClass, Thread.currentThread()
            .getContextClassLoader());
    if (jar != null) {
      conf.setJar(jar);
    }
  }

  public static String findContainingJar(String fileName, ClassLoader loader) {
    try {
      for (Enumeration<?> itr = loader.getResources(fileName); itr
          .hasMoreElements();) {
        URL url = (URL) itr.nextElement();
        logger.info("findContainingJar finds url:" + url);
        if ("jar".equals(url.getProtocol())) {
          String toReturn = url.getPath();
          if (toReturn.startsWith("file:")) {
            toReturn = toReturn.substring("file:".length());
          }
          toReturn = URLDecoder.decode(toReturn, "UTF-8");
          return toReturn.replaceAll("!.*$", "");
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return null;
  }

  public static String findContainingJar(Class<?> my_class, ClassLoader loader) {
    String class_file = my_class.getName().replaceAll("\\.", "/") + ".class";
    return findContainingJar(class_file, loader);
  }

  public static boolean shouldPathBeIgnored(Path path) throws IOException {
    return path.getName().startsWith("_");
  }

  public static JobConf addAllSubPaths(JobConf conf, Path path)
      throws IOException {
    if (shouldPathBeIgnored(path)) {
      throw new IllegalArgumentException(String.format(
          "Path[%s] should be ignored.", path));
    }

    final FileSystem fs = path.getFileSystem(conf);

    if (fs.exists(path)) {
      for (FileStatus status : fs.listStatus(path)) {
        if (!shouldPathBeIgnored(status.getPath())) {
          if (status.isDir()) {
            addAllSubPaths(conf, status.getPath());
          } else {
            FileInputFormat.addInputPath(conf, status.getPath());
          }
        }
      }
    }
    return conf;
  }

  public static void setPropsInJob(Configuration conf, Props props) {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    try {
      props.storeFlattened(output);
      conf.set("azkaban.props", new String(output.toByteArray(), "UTF-8"));
    } catch (IOException e) {
      throw new RuntimeException("This is not possible!", e);
    }
  }

  public static void saveProps(Props props, String file) throws IOException {
    Path path = new Path(file);

    FileSystem fs = null;
    fs = path.getFileSystem(new Configuration());

    saveProps(fs, props, file);
  }

  public static void saveProps(FileSystem fs, Props props, String file)
      throws IOException {
    Path path = new Path(file);

    // create directory if it does not exist.
    Path parent = path.getParent();
    if (!fs.exists(parent))
      fs.mkdirs(parent);

    // write out properties
    OutputStream output = fs.create(path);
    try {
      props.storeFlattened(output);
    } finally {
      output.close();
    }
  }

  /**
   * Compute split size to control number of Mappers.
   * @param fs
   * @param inputPath
   * @param concurrency
   * @return
   * @throws IOException
   */
  public static long computeSplitSize(FileSystem fs, Path inputPath, long concurrency) throws IOException {
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
    logger.info("Total file size: " + sum + " bytes");
    return sum / concurrency;
  }

  public static Schema getAvroSchemaFromAvro(FileSystem fs, Path path) throws IOException {
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
          logger.warn("Failed to get Schema from " + fileStatus.getPath(), e);
          t = e;
        }
      }
    }
    throw new RuntimeException("Failed to create Schema using path " + path, t);
  }

  private static DataFileStream<Object> createAvroDataStream(FileSystem fs, Path path)
      throws IOException {
    if (logger.isDebugEnabled()) {
      logger.debug("path:" + path.toUri().getPath());
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

  public static Schema getSchema(FileSystem fs, Path schemaPath) throws IOException {
    if(!fs.exists(schemaPath) && !fs.isFile(schemaPath)) {
      throw new IllegalArgumentException("Schema file is not found.");
    }
    return new Schema.Parser().parse(fs.open(schemaPath));
  }

  public static void setLoggerOnNodes() {
    ConsoleAppender appender = new ConsoleAppender(new PatternLayout("%d{yyyy-MM-dd HH:mm:ss} %-5p %c:%L - %m%n"));
    appender.activateOptions();
    Logger.getRootLogger().removeAllAppenders();
    Logger.getRootLogger().addAppender(appender);
    Logger.getRootLogger().setLevel(Level.INFO);
  }
}