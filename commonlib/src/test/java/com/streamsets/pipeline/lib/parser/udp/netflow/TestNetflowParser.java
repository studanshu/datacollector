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
package com.streamsets.pipeline.lib.parser.udp.netflow;

import com.google.common.io.Resources;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.lib.util.UDPTestUtil;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

public class TestNetflowParser {

  private static final String TEN_PACKETS = "netflow-v5-file-1";

  private Stage.Context getContext() {
    return ContextInfoCreator.createSourceContext("i", false, OnRecordError.TO_ERROR,
      Collections.<String>emptyList());
  }

  @Test(expected = OnRecordErrorException.class)
  public void testInvalidVersion() throws Exception {
    UnpooledByteBufAllocator allocator = new UnpooledByteBufAllocator(false);
    NetflowParser netflowParser = new NetflowParser(getContext());
    ByteBuf buf = allocator.buffer(4);
    buf.writeShort(0);
    buf.writeShort(0);
    netflowParser.parse(buf, null, null);
  }

  @Test(expected = OnRecordErrorException.class)
  public void testInvalidCountInvalidLength() throws Exception {
    UnpooledByteBufAllocator allocator = new UnpooledByteBufAllocator(false);
    NetflowParser netflowParser = new NetflowParser(getContext());
    ByteBuf buf = allocator.buffer(4);
    buf.writeShort(5);
    buf.writeShort(1);
    netflowParser.parse(buf, null, null);
  }

  @Test(expected = OnRecordErrorException.class)
  public void testInvalidCountZero() throws Exception {
    UnpooledByteBufAllocator allocator = new UnpooledByteBufAllocator(false);
    NetflowParser netflowParser = new NetflowParser(getContext());
    ByteBuf buf = allocator.buffer(4);
    buf.writeShort(5);
    buf.writeShort(0);
    netflowParser.parse(buf, null, null);
  }

  @Test(expected = OnRecordErrorException.class)
  public void testInvalidPacketTooShort1() throws Exception {
    UnpooledByteBufAllocator allocator = new UnpooledByteBufAllocator(false);
    NetflowParser netflowParser = new NetflowParser(getContext());
    ByteBuf buf = allocator.buffer(0);
    netflowParser.parse(buf, null, null);
  }

  @Test(expected = OnRecordErrorException.class)
  public void testInvalidPacketTooShort2() throws Exception {
    UnpooledByteBufAllocator allocator = new UnpooledByteBufAllocator(false);
    NetflowParser netflowParser = new NetflowParser(getContext());
    ByteBuf buf = allocator.buffer(2);
    buf.writeShort(5);
    netflowParser.parse(buf, null, null);
  }

  @Test
  public void testV5() throws Exception {
    UnpooledByteBufAllocator allocator = new UnpooledByteBufAllocator(false);
    NetflowParser netflowParser = new NetflowParser(getContext());
    byte[] bytes = Resources.toByteArray(Resources.getResource(TEN_PACKETS));
    ByteBuf buf = allocator.buffer(bytes.length);
    buf.writeBytes(bytes);
    List<Record> records = netflowParser.parse(buf, null, null);
    UDPTestUtil.assertRecordsForTenPackets(records);
  }

}
