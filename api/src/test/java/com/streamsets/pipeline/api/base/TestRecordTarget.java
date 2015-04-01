/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.base;

import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

public class TestRecordTarget {

  @Test
  public void testTarget() throws Exception {
    Record record1 = Mockito.mock(Record.class);
    Record record2 = Mockito.mock(Record.class);
    Batch batch = Mockito.mock(Batch.class);
    Mockito.when(batch.getRecords()).thenReturn(ImmutableSet.of(record1, record2).iterator());

    final List<Record> got = new ArrayList<Record>();

    Target target = new RecordTarget() {
      @Override
      protected void write(Record record) throws StageException {
        got.add(record);
      }
    };
    target.write(batch);

    Assert.assertEquals(2, got.size());
    Assert.assertEquals(record1, got.get(0));
    Assert.assertEquals(record2, got.get(1));
  }

  public enum ERROR implements ErrorCode {
    ERR;

    @Override
    public String getCode() {
      return ERR.name();
    }

    @Override
    public String getMessage() {
      return "error";
    }

  }

  @Test
  public void testOnErrorHandlingDiscard() throws Exception {
    final Record record1 = Mockito.mock(Record.class);
    final Record record2 = Mockito.mock(Record.class);
    Batch batch = Mockito.mock(Batch.class);
    Mockito.when(batch.getRecords()).thenReturn(ImmutableSet.of(record1, record2).iterator());

    final List<Record> got = new ArrayList<Record>();

    Target target = new RecordTarget() {
      @Override
      protected void write(Record record) throws StageException {
        if (record == record1) {
          got.add(record);
        } else {
          throw new OnRecordErrorException(ERROR.ERR);
        }
      }
    };

    Stage.Info info = Mockito.mock(Stage.Info.class);
    Target.Context context = Mockito.mock(Target.Context.class);
    Mockito.when(context.getOnErrorRecord()).thenReturn(OnRecordError.DISCARD);
    target.init(info, context);
    target.write(batch);
    target.destroy();
    Assert.assertEquals(1, got.size());
    Assert.assertEquals(record1, got.get(0));
    Mockito.verify(context, Mockito.never()).toError((Record) Mockito.any(), (Exception) Mockito.any());
  }

  @Test
  public void testOnErrorHandlingToError() throws Exception {
    final Record record1 = Mockito.mock(Record.class);
    final Record record2 = Mockito.mock(Record.class);
    Batch batch = Mockito.mock(Batch.class);
    Mockito.when(batch.getRecords()).thenReturn(ImmutableSet.of(record1, record2).iterator());
    final BatchMaker batchMaker = Mockito.mock(BatchMaker.class);

    final List<Record> got = new ArrayList<Record>();

    Target target = new RecordTarget() {
      @Override
      protected void write(Record record) throws StageException {
        if (record == record1) {
          got.add(record);
        } else {
          throw new OnRecordErrorException(ERROR.ERR);
        }
      }
    };

    Stage.Info info = Mockito.mock(Stage.Info.class);
    Target.Context context = Mockito.mock(Target.Context.class);
    Mockito.when(context.getOnErrorRecord()).thenReturn(OnRecordError.TO_ERROR);
    target.init(info, context);
    target.write(batch);
    target.destroy();
    Assert.assertEquals(1, got.size());
    Assert.assertEquals(record1, got.get(0));
    ArgumentCaptor<Record> captor = ArgumentCaptor.forClass(Record.class);
    Mockito.verify(context, Mockito.times(1)).toError(captor.capture(), (Exception) Mockito.any());
    Assert.assertEquals(record2, captor.getValue());
  }

  @Test(expected = OnRecordErrorException.class)
  public void testOnErrorHandlingStopPipeline() throws Exception {
    final Record record1 = Mockito.mock(Record.class);
    final Record record2 = Mockito.mock(Record.class);
    Batch batch = Mockito.mock(Batch.class);
    Mockito.when(batch.getRecords()).thenReturn(ImmutableSet.of(record1, record2).iterator());
    final BatchMaker batchMaker = Mockito.mock(BatchMaker.class);

    final List<Record> got = new ArrayList<Record>();

    Target target = new RecordTarget() {
      @Override
      protected void write(Record record) throws StageException {
        if (record == record1) {
          got.add(record);
        } else {
          throw new OnRecordErrorException(ERROR.ERR);
        }
      }
    };

    Stage.Info info = Mockito.mock(Stage.Info.class);
    Target.Context context = Mockito.mock(Target.Context.class);
    Mockito.when(context.getOnErrorRecord()).thenReturn(OnRecordError.STOP_PIPELINE);
    target.init(info, context);
    target.write(batch);
  }

  private void testOnErrorHandlingOtherException(OnRecordError onRecordError) throws Exception {
    final Record record1 = Mockito.mock(Record.class);
    final Record record2 = Mockito.mock(Record.class);
    Batch batch = Mockito.mock(Batch.class);
    Mockito.when(batch.getRecords()).thenReturn(ImmutableSet.of(record1, record2).iterator());
    final BatchMaker batchMaker = Mockito.mock(BatchMaker.class);

    final List<Record> got = new ArrayList<Record>();

    Target target = new RecordTarget() {
      @Override
      protected void write(Record record) throws StageException {
        throw new RuntimeException();
      }
    };

    Stage.Info info = Mockito.mock(Stage.Info.class);
    Target.Context context = Mockito.mock(Target.Context.class);
    Mockito.when(context.getOnErrorRecord()).thenReturn(onRecordError);
    target.init(info, context);
    target.write(batch);
  }

  @Test(expected = Exception.class)
  public void testOnErrorHandlingDiscardOtherException() throws Exception {
    testOnErrorHandlingOtherException(OnRecordError.DISCARD);
  }

  @Test(expected = Exception.class)
  public void testOnErrorHandlingToErrorOtherException() throws Exception {
    testOnErrorHandlingOtherException(OnRecordError.TO_ERROR);
  }

  @Test(expected = Exception.class)
  public void testOnErrorHandlingStopPipelineOtherException() throws Exception {
    testOnErrorHandlingOtherException(OnRecordError.STOP_PIPELINE);
  }

}