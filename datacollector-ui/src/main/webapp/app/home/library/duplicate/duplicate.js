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
 * Controller for Library Pane Duplicate Modal.
 */

angular
  .module('dataCollectorApp.home')
  .controller('DuplicateModalInstanceController', function ($scope, $modalInstance, pipelineInfo, api, $q) {
    angular.extend($scope, {
      common: {
        errors: []
      },
      newConfig : {
        name: pipelineInfo.name + 'copy',
        description: pipelineInfo.description,
        numberOfCopies: 1
      },
      operationInProgress: false,
      save : function () {
        if ($scope.newConfig.numberOfCopies === 1) {
          $scope.operationInProgress = true;
          $q.when(api.pipelineAgent.duplicatePipelineConfig($scope.newConfig.name, $scope.newConfig.description,
            pipelineInfo)).
          then(function(configObject) {
            $modalInstance.close(configObject);
          },function(res) {
            $scope.operationInProgress = false;
            $scope.common.errors = [res.data];
          });
        } else {
          $scope.operationInProgress = true;
          var deferList = [];
          for (var i = 0; i < $scope.newConfig.numberOfCopies; i++) {
            deferList.push(api.pipelineAgent.duplicatePipelineConfig(
              $scope.newConfig.name + (i + 1),
              $scope.newConfig.description,
              pipelineInfo
            ));
          }
          $q.all(deferList).
          then(function(configObjects) {
            $modalInstance.close(configObjects);
          },function(res) {
            $scope.operationInProgress = false;
            $scope.common.errors = [res.data];
          });
        }
      },
      cancel : function () {
        $modalInstance.dismiss('cancel');
      }
    });
  });
