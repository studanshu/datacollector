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
 * Controller for Download Remote Pipeline Modal.
 */

angular
  .module('dataCollectorApp.home')
  .controller('DownloadRemoteModalInstanceController', function ($scope, $modalInstance, api, authService) {
    angular.extend($scope, {
      remoteBaseUrl: authService.getRemoteBaseUrl(),
      common: {
        errors: []
      },
      remotePipelines: [],
      sortColumn: 'name',
      sortReverse: true,
      overwrite: false,
      downloaded: {

      },
      downloading: {

      },
      downloadRemotePipeline : function(remotePipeline) {
        $scope.downloading[remotePipeline.commitId] = true;
        api.remote.getPipeline(authService.getRemoteBaseUrl(), authService.getSSOToken(), remotePipeline)
          .then(
            function(res) {
              var remotePipeline = res.data;
              var pipelineEnvelope = {
                pipelineConfig: JSON.parse(remotePipeline.pipelineDefinition),
                pipelineRules: JSON.parse(remotePipeline.currentRules.rulesDefinition)
              };

              api.pipelineAgent.importPipelineConfig(remotePipeline.name, pipelineEnvelope, $scope.overwrite)
                .then(
                  function(res) {
                    $scope.downloading[remotePipeline.commitId] = false;
                    $scope.downloaded[remotePipeline.commitId] = true;
                  },
                  function(res) {
                    $scope.common.errors = [res.data];
                    $scope.downloading[remotePipeline.commitId] = false;
                  }
                );
            },
            function(res) {
              $scope.common.errors = [res.data];
            }
          );
      },
      close : function () {
        $modalInstance.close();
      }
    });

    var fetchRemotePipelines = function() {
      api.remote.fetchPipelines(authService.getRemoteBaseUrl(), authService.getSSOToken())
        .then(
          function(res) {
            $scope.remotePipelines = res.data;
          },
          function(res) {
            $scope.common.errors = [res.data];
          }
        );
    };

    fetchRemotePipelines();

  });
