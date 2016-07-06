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

/**
 * Data Collector Constants.
 */
angular.module('dataCollectorApp.common')
  .constant('pipelineConstant', {
    SOURCE_STAGE_TYPE : 'SOURCE',
    PROCESSOR_STAGE_TYPE : 'PROCESSOR',
    SELECTOR_STAGE_TYPE : 'SELECTOR',
    TARGET_STAGE_TYPE : 'TARGET',
    STAGE_INSTANCE: 'STAGE_INSTANCE',
    LINK: 'LINK',
    PIPELINE: 'PIPELINE',
    DENSITY_COMFORTABLE: 'COMFORTABLE',
    DENSITY_COZY: 'COZY',
    DENSITY_COMPACT: 'COMPACT',
    LOCAL_HELP: 'LOCAL_HELP',
    HOSTED_HELP: 'HOSTED_HELP',
    CONFIGURED_SOURCE: 'CONFIGURED_SOURCE',
    SNAPSHOT_SOURCE: 'SNAPSHOT_SOURCE',
    USER_PROVIDED: 'USER_PROVIDED',
    STANDALONE: 'STANDALONE',
    CLUSTER: 'CLUSTER',
    CLUSTER_BATCH: 'CLUSTER_BATCH',
    CLUSTER_YARN_STREAMING: 'CLUSTER_YARN_STREAMING',
    CLUSTER_MESOS_STREAMING: 'CLUSTER_MESOS_STREAMING',
    SLAVE: 'SLAVE',
    NON_LIST_MAP_ROOT: 'root',

    BUTTON_CATEGORY: 'button',
    TAB_CATEGORY: 'tab',
    STAGE_CATEGORY: 'stage',
    CLICK_ACTION: 'click',
    SELECT_ACTION: 'select',
    ADD_ACTION: 'add',
    CONNECT_ACTION: 'connect'
  });