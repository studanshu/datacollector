/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.base;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

public class TestSingleLaneRecordProcessor {

  @Test
  public void testProcessor() throws Exception {
    Record record1 = Mockito.mock(Record.class);
    Record record2 = Mockito.mock(Record.class);
    Batch batch = Mockito.mock(Batch.class);
    Mockito.when(batch.getRecords()).thenReturn(ImmutableSet.of(record1, record2).iterator());
    final BatchMaker batchMaker = Mockito.mock(BatchMaker.class);

    final List<Record> got = new ArrayList<Record>();

    Processor processor = new SingleLaneRecordProcessor() {
      @Override
      protected void process(Record record, SingleLaneBatchMaker batchMaker) {
        got.add(record);
      }
    };

    Stage.Info info = Mockito.mock(Stage.Info.class);
    Processor.Context context = Mockito.mock(Processor.Context.class);
    Mockito.when(context.getOutputLanes()).thenReturn(ImmutableList.of("l2"));
    processor.init(info, context);

    processor.process(batch, batchMaker);

    Assert.assertEquals(ImmutableList.of(record1, record2), got);
  }

}