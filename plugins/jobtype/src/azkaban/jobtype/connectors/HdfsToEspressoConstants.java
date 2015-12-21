package azkaban.jobtype.connectors;

public interface HdfsToEspressoConstants {
  public static final String ESPRESSO_ENDPOINT_KEY = "espresso.endpoint";
  public static final String ESPRESSO_DB_KEY = "espresso.database";
  public static final String ESPRESSO_TABLE_KEY = "espresso.table";
  public static final String ESPRESSO_CONTENT_TYPE_KEY = "espresso.content.type";
  public static final String ESPRESSO_KEY = "espresso.key";
  public static final String ESPRESSO_SUBKEY = "espresso.subkey";
  public static final String ESPRESSO_IS_MULTIPUT_KEY = "espresso.is.multiput";
  public static final String ESPRESSO_PUT_QPS_KEY = "espresso.qps";
  public static final int ESPRESSO_MAX_PUT_QPS = 6000;

  public static final String MR_NUM_THREAD_KEY = "node.no.putthread";
  public static final String MR_NUM_NODES_KEY = "no.nodes";
  public static final int DEFAULT_NO_NODES = 6;
  public static final int MR_NODE_MAX_THREADS = 100;

  public static final double FAIL_FAST_PERCENTAGE = 5D;
  public static final long FAIL_FAST_NUM_FAILURE = 100L;
  public static final long FAIL_FAST_REQUEST_THRESHOLD = 200L;
}