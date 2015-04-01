/**
 * Service for providing access to the backend API via HTTP.
 */

angular.module('dataCollectorApp.common')
  .factory('api', function($rootScope, $http, $q) {
    var apiBase = '/rest/v1',
      api = {events: {}};

    api.log = {
      /**
       * Fetch current log
       *
       * @param endingOffset
       */
      getCurrentLog: function(endingOffset) {
        var url = apiBase + '/log?endingOffset=' +  (endingOffset ? endingOffset : '-1');
        return $http({
          method: 'GET',
          url: url
        });
      },

      /**
       * Fetch list of Log file names
       *
       * @returns {*}
       */
      getFilesList: function() {
        var url = apiBase + '/log/files';
        return $http({
          method: 'GET',
          url: url
        });
      }
    };

    api.admin = {

      /**
       * Fetch Help IDs
       */
      getHelpRef: function() {
        var url = apiBase + '/helpref';
        return $http({
          method: 'GET',
          url: url
        });
      },

      /**
       * Fetches JVM Metrics
       * @returns {*}
       */
      getJMX : function() {
        var url = '/jmx';
        return $http({
          method: 'GET',
          url: url
        });
      },


      /**
       * Fetches JVM Thread Dump
       */
      getThreadDump: function() {
        var url = apiBase + '/admin/threadsDump';
        return $http({
          method: 'GET',
          url: url
        });
      },

      /**
       * Fetched User Information
       */
      getUserInfo: function() {
        var url = apiBase + '/info/user';
        return $http({
          method: 'GET',
          url: url
        });
      },

      /**
       * Fetched Build Information
       */
      getBuildInfo: function() {
        var url = apiBase + '/info/sdc';
        return $http({
          method: 'GET',
          url: url
        });
      },

      /**
       * Shutdown the Data Collector.
       * @returns {*}
       */
      shutdownCollector: function() {
        var url = apiBase + '/admin/shutdown';
        return $http({
          method: 'POST',
          url: url
        });
      },

      /**
       * logout
       */
      logout: function() {
        var url = apiBase + '/logout';
        return $http({
          method: 'POST',
          url: url
        });
      }

    };

    api.pipelineAgent = {
      /**
       * Fetches Configuration from dist/src/main/etc/pipeline.properties
       *
       * @returns {*}
       */
      getConfiguration: function() {
        var url = apiBase + '/configuration/all';
        return $http({
          method: 'GET',
          url: url
        });
      },

      /**
       * Fetches all configuration definitions of Pipeline and Stage Configuration.
       *
       * @returns {*}
       */
      getDefinitions: function() {
        var url = apiBase + '/definitions';
        return $http({
          method: 'GET',
          url: url
        });
      },

      /**
       * Fetches all Pipeline Configuration Info.
       *
       * @returns {*}
       */
      getPipelines: function() {
        var url = apiBase + '/pipeline-library';
        return $http({
          method: 'GET',
          url: url
        });
      },

      /**
       * Fetches Pipeline Configuration.
       *
       * @param name
       * @returns {*}
       */
      getPipelineConfig: function(name) {
        var url;

        if(!name) {
          name = 'xyz';
        }

        url = apiBase + '/pipeline-library/' + name;
        return $http({
          method: 'GET',
          url: url
        });
      },

      /**
       * Fetches Pipeline Configuration Information
       *
       * @param name
       * @returns {*}
       */
      getPipelineConfigInfo: function(name) {
        var url;

        if(!name) {
          name = 'xyz';
        }

        url = apiBase + '/pipeline-library/' + name + '?get=info';
        return $http({
          method: 'GET',
          url: url
        });
      },

      /**
       * Sends updated Pipeline configuration to server for update.
       *
       * @param name - Pipeline Name
       * @param config - Modified Pipeline Configuration
       * @returns Updated Pipeline Configuration
       */
      savePipelineConfig: function(name, config) {
        var url;

        if(!name) {
          name = 'xyz';
        }

        url = apiBase + '/pipeline-library/' + name;
        return $http({
          method: 'POST',
          url: url,
          data: config
        });
      },

      /**
       * Create new Pipeline Configuration.
       *
       * @param name
       * @param description
       */
      createNewPipelineConfig: function(name, description) {
        var url = apiBase + '/pipeline-library/' + name + '?description=' + description;

        return $http({
          method: 'PUT',
          url: url
        });
      },

      /**
       * Delete Pipeline Cofiguration.
       *
       * @param name
       * @returns {*}
       */
      deletePipelineConfig: function(name) {
        var url = apiBase + '/pipeline-library/' + name;

        return $http({
          method: 'DELETE',
          url: url
        });
      },


      duplicatePipelineConfig: function(name, description, pipelineInfo) {
        var deferred = $q.defer(),
          pipelineObject,
          pipelineRulesObject,
          duplicatePipelineObject,
          duplicatePipelineRulesObject;

        // Fetch the pipelineInfo full object
        // then Create new config object
        // then copy the configuration from pipelineInfo to new Object.
        $q.all([api.pipelineAgent.getPipelineConfig(pipelineInfo.name), api.pipelineAgent.getPipelineRules(pipelineInfo.name)])
          .then(function(results) {
            pipelineObject = results[0].data;
            pipelineRulesObject = results[1].data;
            return api.pipelineAgent.createNewPipelineConfig(name, description);
          })
          .then(function(res) {
            duplicatePipelineObject = res.data;
            duplicatePipelineObject.configuration = pipelineObject.configuration;
            duplicatePipelineObject.uiInfo = pipelineObject.uiInfo;
            duplicatePipelineObject.errorStage = pipelineObject.errorStage;
            duplicatePipelineObject.stages = pipelineObject.stages;
            return api.pipelineAgent.savePipelineConfig(name, duplicatePipelineObject);
          })
          .then(function(res) {
            duplicatePipelineObject = res.data;

            //Fetch the Pipeline Rules
            return api.pipelineAgent.getPipelineRules(name);
          })
          .then(function(res) {
            duplicatePipelineRulesObject = res.data;
            duplicatePipelineRulesObject.metricsRuleDefinitions = pipelineRulesObject.metricsRuleDefinitions;
            duplicatePipelineRulesObject.dataRuleDefinitions = pipelineRulesObject.dataRuleDefinitions;
            duplicatePipelineRulesObject.emailIds = pipelineRulesObject.emailIds;

            //Save the pipeline Rules
            return api.pipelineAgent.savePipelineRules(name, duplicatePipelineRulesObject);
          })
          .then(function(res) {
            deferred.resolve(duplicatePipelineObject);
          },function(res) {
            deferred.reject(res);
          });

        return deferred.promise;
      },


      /**
       * Export Pipeline Configuration.
       *
       * @param name
       */
      exportPipelineConfig: function(name) {
        var url;

        if(!name) {
          name = 'xyz';
        }

        url = apiBase + '/pipeline-library/' + name + '?attachment=true';

        window.open(url, '_blank', '');
      },

      /**
       * Fetches Preview Data for Pipeline
       *
       * @param name
       * @param sourceOffset
       * @param batchSize
       * @param rev
       * @param skipTargets
       * @param stageOutputList
       * @returns {*}
       */
      previewPipeline: function(name, sourceOffset, batchSize, rev, skipTargets, stageOutputList) {
        var url;

        if(!sourceOffset) {
          sourceOffset = 0;
        }

        if(!batchSize) {
          batchSize = 10;
        }

        url = apiBase + '/pipeline-library/' + name + '/preview?sourceOffset=' + sourceOffset +
          '&batchSize=' + batchSize + '&rev=' + rev + '&skipTargets=' + skipTargets;

        return $http({
          method: 'POST',
          url: url,
          data: stageOutputList || []
        });
      },

      /**
       * Fetch the Pipeline Status
       *
       * @returns {*}
       */
      getPipelineStatus: function() {
        var url = apiBase + '/pipeline/status';
        return $http({
          method: 'GET',
          url: url
        });
      },

      /**
       * Validate the Pipeline
       *
       * @param name
       * @returns {*}
       */
      validatePipeline: function(name) {
        var url = apiBase + '/pipeline-library/' + name + '/validateConfigs';
        return $http({
          method: 'GET',
          url: url
        });
      },

      /**
       * Start the Pipeline
       *
       * @param name
       * @param rev
       * @returns {*}
       */
      startPipeline: function(name, rev) {
        var url = apiBase + '/pipeline/start?name=' + name + '&rev=' + rev ;
        return $http({
          method: 'POST',
          url: url
        });
      },

      /**
       * Stop the Pipeline
       *
       * @returns {*}
       */
      stopPipeline: function() {
        var url = apiBase + '/pipeline/stop';
        return $http({
          method: 'POST',
          url: url
        });
      },

      /**
       * Fetch the Pipeline Metrics
       *
       * @returns {*}
       */
      getPipelineMetrics: function() {
        var url = apiBase + '/pipeline/metrics';
        return $http({
          method: 'GET',
          url: url
        });
      },

      /**
       * Get List of available snapshots.
       *
       * @returns {*}
       */
      getSnapshotsInfo: function() {
        var url = apiBase + '/pipeline/snapshots' ;
        return $http({
          method: 'GET',
          url: url
        });
      },

      /**
       * Capture Snapshot of running pipeline.
       *
       * @param snapshotName
       * @param batchSize
       * @returns {*}
       */
      captureSnapshot: function(snapshotName, batchSize) {
        var url = apiBase + '/pipeline/snapshots/' + snapshotName + '?batchSize=' + batchSize ;
        return $http({
          method: 'PUT',
          url: url
        });
      },

      /**
       * Get Status of Snapshot.
       *
       * @param snapshotName
       * @returns {*}
       */
      getSnapshotStatus: function(snapshotName) {
        var url = apiBase + '/pipeline/snapshots/' + snapshotName;
        return $http({
          method: 'GET',
          url: url
        });
      },

      /**
       * Get captured snapshot for given pipeline name.
       *
       * @param pipelineName
       * @param snapshotName
       * @returns {*}
       */
      getSnapshot: function(pipelineName, snapshotName) {
        var url = apiBase + '/pipeline/snapshots/' + pipelineName + '/' + snapshotName ;
        return $http({
          method: 'GET',
          url: url
        });
      },

      /**
       * Delete captured snapshot for given pipeline name.
       *
       * @param pipelineName
       * @param snapshotName
       * @returns {*}
       */
      deleteSnapshot: function(pipelineName, snapshotName) {
        var url = apiBase + '/pipeline/snapshots/' + pipelineName + '/' + snapshotName ;
        return $http({
          method: 'DELETE',
          url: url
        });
      },


      /**
       * Get error records for the given stage instance name of running pipeline if it is provided otherwise
       * return error records for the pipeline.
       *
       * @param stageInstanceName
       * @returns {*}
       */
      getErrorRecords: function(stageInstanceName) {
        var url = apiBase + '/pipeline/errorRecords?stageInstanceName=' + stageInstanceName;
        return $http({
          method: 'GET',
          url: url
        });
      },

      /**
       * Get error messages for the given stage instance name of running pipeline if is provided otherwise
       * return error messages for the pipeline.
       *
       * @param stageInstanceName
       * @returns {*}
       */
      getErrorMessages: function(stageInstanceName) {
        var url = apiBase + '/pipeline/errorMessages?stageInstanceName=' + stageInstanceName;
        return $http({
          method: 'GET',
          url: url
        });
      },

      /**
       * Raw Source Preview
       *
       * @param name
       * @param rev
       * @param configurations
       * @returns {*}
       */
      rawSourcePreview: function(name, rev, configurations) {
        var url = apiBase + '/pipeline-library/' + name + '/rawSourcePreview?rev=' + rev;

        angular.forEach(configurations, function(config) {
          if(config.name && config.value !== undefined) {
            url+= '&' + config.name + '=' + config.value;
          }
        });

        return $http({
          method: 'GET',
          url: url
        });
      },

      /**
       * Get history of the pipeline
       *
       * @param name
       * @param rev
       * @returns {*}
       */
      getHistory: function(name, rev) {
        var url = apiBase + '/pipeline/history/' + name;
        return $http({
          method: 'GET',
          url: url
        });
      },

      /**
       * Reset Offset for Pipeline
       *
       * @param name
       */
      resetOffset: function(name) {
        var url = apiBase + '/pipeline/resetOffset/' + name;
        return $http({
          method: 'POST',
          url: url
        });
      },

      /**
       * Fetches Pipeline Rules.
       *
       * @param name
       * @returns {*}
       */
      getPipelineRules: function(name) {
        var url;

        url = apiBase + '/pipeline-library/' + name + '/rules';
        return $http({
          method: 'GET',
          url: url
        });
      },

      /**
       * Sends updated Pipeline rules to server for update.
       *
       * @param name - Pipeline Name
       * @param rules - Modified Pipeline Configuration
       * @returns Updated Pipeline Rules
       */
      savePipelineRules: function(name, rules) {
        var url = apiBase + '/pipeline-library/' + name + '/rules';
        return $http({
          method: 'POST',
          url: url,
          data: rules
        });
      },


      /**
       * Get Sampled data for given sampling rule id.
       *
       * @param samplingRuleId
       * @returns {*}
       */
      getSampledRecords: function(samplingRuleId) {
        var url = apiBase + '/pipeline/sampledRecords/?sampleId=' + samplingRuleId ;
        return $http({
          method: 'GET',
          url: url
        });
      },

      /**
       * Delete Alert
       *
       * @param name
       * @param ruleId
       * @returns {*}
       */
      deleteAlert: function(name, ruleId) {
        var url = apiBase + '/pipeline/alerts/' + name + '?alertId=' + ruleId;

        return $http({
          method: 'DELETE',
          url: url
        });
      }
    };

    return api;
  });