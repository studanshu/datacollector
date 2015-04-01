/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.prodmanager;

public enum State {

  STOPPED,
  RUNNING,
  STOPPING,
  ERROR,
  FINISHED,
  NODE_PROCESS_SHUTDOWN
}