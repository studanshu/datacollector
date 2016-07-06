/**
 * Copyright 2016 StreamSets Inc.
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
package com.streamsets.datacollector.event.binding;

import com.streamsets.datacollector.event.dto.AckEvent;
import com.streamsets.datacollector.event.dto.ClientEvent;
import com.streamsets.datacollector.event.dto.PingFrequencyAdjustmentEvent;
import com.streamsets.datacollector.event.dto.PipelineBaseEvent;
import com.streamsets.datacollector.event.dto.PipelineSaveEvent;
import com.streamsets.datacollector.event.dto.PipelineSaveRulesEvent;
import com.streamsets.datacollector.event.dto.PipelineStatusEvent;
import com.streamsets.datacollector.event.dto.SDCInfoEvent;
import com.streamsets.datacollector.event.dto.ServerEvent;
import com.streamsets.datacollector.event.json.AckEventJson;
import com.streamsets.datacollector.event.json.ClientEventJson;
import com.streamsets.datacollector.event.json.PingFrequencyAdjustmentEventJson;
import com.streamsets.datacollector.event.json.PipelineBaseEventJson;
import com.streamsets.datacollector.event.json.PipelineSaveEventJson;
import com.streamsets.datacollector.event.json.PipelineSaveRulesEventJson;
import com.streamsets.datacollector.event.json.PipelineStatusEventJson;
import com.streamsets.datacollector.event.json.SDCInfoEventJson;
import com.streamsets.datacollector.event.json.ServerEventJson;

import fr.xebia.extras.selma.Mapper;
import fr.xebia.extras.selma.Maps;
import fr.xebia.extras.selma.Selma;

@Mapper
public abstract class MessagingDtoJsonMapper {
  public static final MessagingDtoJsonMapper INSTANCE = Selma.builder(MessagingDtoJsonMapper.class).build();

  public abstract PipelineSaveEventJson toPipelineSaveEventJson(PipelineSaveEvent pipelineSaveEvent);

  public abstract PipelineSaveEvent asPipelineSaveEventDto(PipelineSaveEventJson pipelineSaveEventJson);

  public abstract PipelineSaveRulesEventJson toPipelineSaveRulesEventJson(PipelineSaveRulesEvent pipelineSaveRulesEvent);

  public abstract PipelineSaveRulesEvent asPipelineSaveRulesEventDto(PipelineSaveRulesEventJson pipelineSaveRulesEventJson);

  public abstract PipelineBaseEventJson toPipelineBaseEventJson(PipelineBaseEvent event);

  public abstract PipelineBaseEvent asPipelineBaseEventDto(PipelineBaseEventJson pipelineActionEventJson);

  public abstract PingFrequencyAdjustmentEventJson toPingFrequencyAdjustmentEventJson(PingFrequencyAdjustmentEvent pingFrequencyEvent);

  public abstract PingFrequencyAdjustmentEvent asPingFrequencyAdjustmentEventDto(PingFrequencyAdjustmentEventJson pingFrequencyEventJson);

  public abstract PipelineStatusEventJson toPipelineStatusEventJson(PipelineStatusEvent pipelineStatusEvent);

  public abstract AckEventJson toAckEventJson(AckEvent ackEvent);

  public abstract AckEvent asAckEventDto(AckEventJson ackEvent);

  public abstract PipelineStatusEvent asPipelineStatusEventDto(PipelineStatusEventJson pipelineStatusEvent);

  public abstract SDCInfoEvent asSDCInfoEventDto(SDCInfoEventJson sdcInfoEventJson);

  public abstract SDCInfoEventJson toSDCInfoEventJson(SDCInfoEvent sdcInfoEvent);

  @Maps(withIgnoreFields = {"payload", "event"})
  public abstract ServerEvent asServerEventDto(ServerEventJson serverEventJson);

  @Maps(withIgnoreFields = {"payload", "event", "eventType"})
  public abstract ClientEventJson toClientEventJson(ClientEvent clientEvent);
}
