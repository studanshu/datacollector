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
package com.streamsets.datacollector.restapi;

import com.streamsets.datacollector.execution.Manager;
import com.streamsets.datacollector.execution.PreviewOutput;
import com.streamsets.datacollector.execution.PreviewStatus;
import com.streamsets.datacollector.execution.Previewer;
import com.streamsets.datacollector.execution.RawPreview;
import com.streamsets.datacollector.restapi.bean.BeanHelper;
import com.streamsets.datacollector.restapi.bean.PreviewInfoJson;
import com.streamsets.datacollector.restapi.bean.PreviewOutputJson;
import com.streamsets.datacollector.restapi.bean.StageOutputJson;
import com.streamsets.datacollector.runner.PipelineRuntimeException;
import com.streamsets.datacollector.store.PipelineStoreException;
import com.streamsets.datacollector.util.AuthzRole;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.util.ContainerError;
import com.streamsets.datacollector.util.PipelineException;
import com.streamsets.pipeline.api.StageException;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.Authorization;

import javax.annotation.security.DenyAll;
import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import java.io.IOException;
import java.security.Principal;
import java.util.Collections;
import java.util.List;

@Path("/v1")
@Api(value = "preview")
@DenyAll
public class PreviewResource {
  private static final String MAX_BATCH_SIZE_KEY = "preview.maxBatchSize";
  private static final int MAX_BATCH_SIZE_DEFAULT = 10;
  private static final String MAX_BATCHES_KEY = "preview.maxBatches";
  private static final int MAX_BATCHES_DEFAULT = 10;

  //preview.maxBatchSize
  private final Manager manager;
  private final Configuration configuration;
  private final String user;

  @Inject
  public PreviewResource(Manager manager, Configuration configuration, Principal principal) {
    this.manager = manager;
    this.configuration = configuration;
    this.user = principal.getName();
  }

  @Path("/pipeline/{pipelineName}/preview")
  @POST
  @ApiOperation(value = "Run Pipeline preview",
    response = PreviewInfoJson.class, authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({
      AuthzRole.CREATOR, AuthzRole.ADMIN, AuthzRole.CREATOR_REMOTE, AuthzRole.ADMIN_REMOTE
  })
  public Response previewWithOverride(
      @PathParam("pipelineName") String pipelineName,
      @QueryParam("rev") String rev,
      @QueryParam("batchSize") @DefaultValue("" + Integer.MAX_VALUE) int batchSize,
      @QueryParam("batches") @DefaultValue("1") int batches,
      @QueryParam("skipTargets") @DefaultValue("true") boolean skipTargets,
      @QueryParam("endStage") String endStageInstanceName,
      @QueryParam("timeout") @DefaultValue("2000") long timeout,
      @ApiParam(name="stageOutputsToOverrideJson", required = true)  List<StageOutputJson> stageOutputsToOverrideJson)
      throws PipelineException, StageException {

    if (stageOutputsToOverrideJson == null) {
      stageOutputsToOverrideJson = Collections.EMPTY_LIST;
    }
    RestAPIUtils.injectPipelineInMDC(pipelineName);
    int maxBatchSize = configuration.get(MAX_BATCH_SIZE_KEY, MAX_BATCH_SIZE_DEFAULT);
    batchSize = Math.min(maxBatchSize, batchSize);
    int maxBatches = configuration.get(MAX_BATCHES_KEY, MAX_BATCHES_DEFAULT);
    batches = Math.min(maxBatches, batches);

    Previewer previewer = manager.createPreviewer(this.user, pipelineName, rev);
    try {
      previewer.start(batches, batchSize, skipTargets, endStageInstanceName,
        BeanHelper.unwrapStageOutput(stageOutputsToOverrideJson), timeout);
      PreviewInfoJson previewInfoJson = new PreviewInfoJson(previewer.getId(), previewer.getStatus());
      return Response.ok().type(MediaType.APPLICATION_JSON).entity(previewInfoJson).build();
    } catch (PipelineRuntimeException ex) {
      if (ex.getErrorCode() == ContainerError.CONTAINER_0165) {
        return Response.status(Response.Status.BAD_REQUEST).type(MediaType.APPLICATION_JSON).entity(
          BeanHelper.wrapIssues(ex.getIssues())).build();
      } else {
        throw ex;
      }
    }
  }

  @Path("/pipeline/{pipelineName}/preview/{previewerId}/status")
  @GET
  @ApiOperation(value = "Return Preview status by previewer ID", response = PreviewInfoJson.class,
    authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({
      AuthzRole.CREATOR, AuthzRole.ADMIN, AuthzRole.CREATOR_REMOTE, AuthzRole.ADMIN_REMOTE
  })
  public Response getPreviewStatus(@PathParam("pipelineName") String pipelineName,
                                   @PathParam("previewerId") String previewerId)
    throws PipelineException, StageException {
    Previewer previewer = manager.getPreviewer(previewerId);
    if(previewer == null) {
      return Response.status(Response.Status.NOT_FOUND).entity("Cannot find previewer with id " + previewerId).build();
    }
    RestAPIUtils.injectPipelineInMDC(previewer.getName());
    PreviewInfoJson previewInfoJson = new PreviewInfoJson(previewer.getId(), previewer.getStatus());
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(previewInfoJson).build();
  }

  @Path("/pipeline/{pipelineName}/preview/{previewerId}")
  @GET
  @ApiOperation(value = "Return Preview Data by previewer ID", response = PreviewOutputJson.class,
    authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({
      AuthzRole.CREATOR, AuthzRole.ADMIN, AuthzRole.CREATOR_REMOTE, AuthzRole.ADMIN_REMOTE
  })
  public Response getPreviewData(@PathParam("pipelineName") String pipelineName,
                                 @PathParam("previewerId") String previewerId)
    throws PipelineException, StageException {
    Previewer previewer = manager.getPreviewer(previewerId);
    if(previewer == null) {
      return Response.status(Response.Status.NOT_FOUND).entity("Cannot find previewer with id " + previewerId).build();
    }
    RestAPIUtils.injectPipelineInMDC(previewer.getName());
    PreviewOutput previewOutput = previewer.getOutput();
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(BeanHelper.wrapPreviewOutput(previewOutput)).build();
  }

  @Path("/pipeline/{pipelineName}/preview/{previewerId}")
  @DELETE
  @ApiOperation(value = "Stop Preview by previewer ID", response = PreviewInfoJson.class,
    authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({
      AuthzRole.CREATOR, AuthzRole.ADMIN, AuthzRole.CREATOR_REMOTE, AuthzRole.ADMIN_REMOTE
  })
  public Response stopPreview(@PathParam("pipelineName") String pipelineName,
                              @PathParam("previewerId") String previewerId)
    throws PipelineException, StageException {
    Previewer previewer = manager.getPreviewer(previewerId);
    if(previewer == null) {
      return Response.status(Response.Status.NOT_FOUND).entity("Cannot find previewer with id " + previewerId).build();
    }
    RestAPIUtils.injectPipelineInMDC(previewer.getName());
    previewer.stop();
    PreviewInfoJson previewInfoJson = new PreviewInfoJson(previewer.getId(), previewer.getStatus());
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(previewInfoJson).build();
  }

  @Path("/pipeline/{pipelineName}/rawSourcePreview")
  @GET
  @ApiOperation(value = "Get raw source preview data for pipeline name and revision", response = RawPreview.class,
    authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({
      AuthzRole.CREATOR, AuthzRole.ADMIN, AuthzRole.CREATOR_REMOTE, AuthzRole.ADMIN_REMOTE
  })
  public Response rawSourcePreview(
      @PathParam("pipelineName") String pipelineName,
      @QueryParam("rev") String rev,
      @Context UriInfo uriInfo) throws PipelineStoreException,
      PipelineRuntimeException, IOException {
    RestAPIUtils.injectPipelineInMDC(pipelineName);

    MultivaluedMap<String, String> previewParams = uriInfo.getQueryParameters();
    Previewer previewer = manager.createPreviewer(this.user, pipelineName, rev);
    RawPreview rawPreview = previewer.getRawSource(4 * 1024, previewParams);
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(rawPreview).build();
  }

  @Path("/pipeline/{pipelineName}/validate")
  @GET
  @ApiOperation(value = "Validate pipeline configuration and return validation status and issues",
    response = PreviewInfoJson.class, authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({
      AuthzRole.CREATOR,
      AuthzRole.MANAGER,
      AuthzRole.ADMIN,
      AuthzRole.CREATOR_REMOTE,
      AuthzRole.MANAGER_REMOTE,
      AuthzRole.ADMIN_REMOTE
  })
  public Response validateConfigs(
      @PathParam("pipelineName") String pipelineName,
      @QueryParam("rev") String rev,
      @QueryParam("timeout") @DefaultValue("2000") long timeout)
      throws PipelineException, StageException {
    RestAPIUtils.injectPipelineInMDC(pipelineName);
    try {
      Previewer previewer = manager.createPreviewer(this.user, pipelineName, rev);
      previewer.validateConfigs(timeout);
      PreviewStatus previewStatus = previewer.getStatus();
      if(previewStatus == null) {
        previewStatus =  PreviewStatus.VALIDATING;
      }
      PreviewInfoJson previewInfoJson = new PreviewInfoJson(previewer.getId(), previewStatus);
      return Response.ok().type(MediaType.APPLICATION_JSON).entity(previewInfoJson).build();
    } catch (PipelineRuntimeException ex) {
      if (ex.getErrorCode() == ContainerError.CONTAINER_0165) {
        return Response.status(Response.Status.BAD_REQUEST).type(MediaType.APPLICATION_JSON).entity(
            BeanHelper.wrapIssues(ex.getIssues())).build();
      } else {
        throw ex;
      }
    }
  }

}
