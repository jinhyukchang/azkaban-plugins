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

package azkaban.jobtype.connectors;

import org.apache.log4j.Logger;

import azkaban.jobtype.HadoopJavaJob;
import azkaban.utils.Props;


public class HdfsToEspressoJob extends HadoopJavaJob {
  private static final Logger logger = Logger.getLogger(HdfsToEspressoJob.class);

  public HdfsToEspressoJob(String jobid, Props sysProps, Props jobProps, Logger log) throws RuntimeException {
    super(jobid, sysProps, jobProps, log);
    jobProps.put("job.class", "azkaban.jobtype.connectors.HdfsToEspressoHadoopJob");
  }
}
