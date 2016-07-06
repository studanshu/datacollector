/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.datacollector.config.json;

public enum PipelineStatusJson {

  EDITED,          // pipeline job has been create/modified, didn't run since the creation/modification

  STARTING,         // pipeline job starting (initialization)
  START_ERROR,      // pipeline job failed while start (during initialization)

  RUNNING,          // pipeline job running
  RUNNING_ERROR,    // pipeline job failed while running (calling destroy on pipeline)
  RUN_ERROR,        // pipeline job failed while running (done)

  FINISHING,        // pipeline job finishing (source reached end, returning NULL offset) (calling destroy on pipeline)
  FINISHED,         // pipeline job finished                                              (done)
  RETRY,
  KILLED,           // only happens in cluster mode


  STOPPING,         // pipeline job has been manually stopped (calling destroy on pipeline)
  STOPPED,          // pipeline job has been manually stopped (done)

  DISCONNECTING,    // SDC going down gracefully (calling destroy on pipeline for LOCAL, doing nothing for CLUSTER)
  DISCONNECTED,     // SDC going down gracefully (done)

  CONNECTING,       // SDC starting back (transition to STARTING for LOCAL, for CLUSTER checks job still running)
                    //                   (and transitions to RUNNING or RUN_ERROR -streaming- or FINISHED -batch)
  CONNECT_ERROR     // failed to get to RUNNING, on SDC restart will retry again
  ;

}