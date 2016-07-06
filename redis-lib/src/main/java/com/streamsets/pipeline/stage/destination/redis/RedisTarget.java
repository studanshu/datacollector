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
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.streamsets.pipeline.stage.destination.redis;

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactory;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;

public class RedisTarget extends BaseTarget {
  private ErrorRecordHandler errorRecordHandler;
  private DataGeneratorFactory generatorFactory;
  private JedisPool pool;
  private Jedis jedis;
  private static final Logger LOG = LoggerFactory.getLogger(RedisTarget.class);
  private final RedisTargetConfig conf;

  public RedisTarget(RedisTargetConfig conf) {
    this.conf = conf;
  }

  @Override
  public List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();

    JedisPoolConfig poolConfig = new JedisPoolConfig();

    try {
      pool = new JedisPool(poolConfig, URI.create(conf.uri), conf.connectionTimeout);
      String userInfo = URI.create(conf.uri).getUserInfo();
      jedis = pool.getResource();
      if(userInfo != null && userInfo.split(":", 2).length > 0) {
        jedis.clientSetname(userInfo.split(":",2)[0]);
      }
      jedis.ping();
    } catch (JedisException e) { // NOSONAR
      LOG.error(Errors.REDIS_01.getMessage(), e.toString());
      issues.add(getContext().createConfigIssue("REDIS", "conf.uri", Errors.REDIS_01, conf.uri, e.toString()));
    } catch (IllegalArgumentException e) {
      LOG.error(Errors.REDIS_02.getMessage(), e.toString());
      issues.add(getContext().createConfigIssue("REDIS", "conf.uri", Errors.REDIS_02, conf.uri, e.toString()));
    } finally {
      try {
        if (pool != null) {
          pool.close();
        }
      } catch (JedisException ignored) { // NOSONAR
      }
    }

    // Input Validation Check
    if(conf.mode == ModeType.BATCH) {
      if(conf.redisFieldMapping.isEmpty()) {
        LOG.error(Errors.REDIS_04.getMessage(), "conf.redisFieldMapping is required for Batch Mode");
        issues.add(
            getContext().createConfigIssue(
                "REDIS",
                "conf.redisFieldMapping",
                Errors.REDIS_04,
                conf.redisFieldMapping,
                Errors.REDIS_04.getMessage()
            )
        );
      }
    } else if(conf.mode == ModeType.PUBLISH) {
      if(conf.channel.isEmpty()) {
        LOG.error(Errors.REDIS_04.getMessage(), "conf.channel is required for Publish Mode");
        issues.add(
            getContext().createConfigIssue(
                "REDIS",
                "conf.channel",
                Errors.REDIS_03,
                conf.channel,
                Errors.REDIS_03.getMessage()
            )
        );
      }

      conf.dataFormatConfig.init(
          getContext(),
          conf.dataFormat,
          Groups.REDIS.name(),
          "conf.RedisTargetConfig.",
          issues
      );
    }

    if(issues.isEmpty()) {
      generatorFactory = conf.dataFormatConfig.getDataGeneratorFactory();
      errorRecordHandler = new DefaultErrorRecordHandler(getContext());
    }

    return issues;
  }

  @Override
  public void write(Batch batch) throws StageException {
    if (!batch.getRecords().hasNext()) {
      // empty batch
      return;
    }

    if(conf.mode == ModeType.BATCH) {
      doBatch(batch);
    } else if(conf.mode == ModeType.PUBLISH) {
      doPublish(batch);
    }
  }

  @Override
  public void destroy() {
    if(jedis != null) {
      jedis.close();
    }
    if(pool != null) {
      pool.close();
    }
    super.destroy();
  }

  private static class ErrorRecord {
    private final Record record;
    private final String key;
    private final Object value;
    private final String operation;

    private ErrorRecord(Record record, String operation, String key, Object value) {
      this.record = record;
      this.operation = operation;
      this.key = key;
      this.value = value;
    }
  }

  private void doBatch(Batch batch) throws StageException {
    Iterator<Record> records = batch.getRecords();
    Pipeline p = jedis.pipelined();
    List<ErrorRecord> tempRecord = new ArrayList<>();

    while(records.hasNext()) {
      Record record = records.next();
      for(RedisFieldMappingConfig parameters : conf.redisFieldMapping) {
        String key = null;
        if (record.has(parameters.keyExpr)) {
          key = record.get(parameters.keyExpr).getValueAsString();
        }
        Field value = record.get(parameters.valExpr);

        if(key != null && value != null) {
          switch (parameters.dataType) {
            case STRING:
              doUpsertString(record, tempRecord, p, key, value);
              break;
            case LIST:
              doUpsertList(record, tempRecord, p, key, value);
              break;
            case SET:
              doUpsertSet(record, tempRecord, p, key, value);
              break;
            case HASH:
              doUpsertHash(record, tempRecord, p, key, value);
              break;
            default:
              LOG.error(Errors.REDIS_05.getMessage(), parameters.dataType);
              errorRecordHandler.onError(
                  new OnRecordErrorException(
                      record,
                      Errors.REDIS_05,
                      parameters.dataType
                  )
              );
              break;
          }
        } else {
          LOG.warn(Errors.REDIS_07.getMessage(), parameters.keyExpr, parameters.valExpr, record);
        }
      }
    }

    List<Object> results = p.syncAndReturnAll();
    int index = 0;
    for(Object result : results) {
      if(!("OK".equals(result) || Long.class.equals(result == null ? null : result.getClass()))) {
        LOG.error(
            Errors.REDIS_03.getMessage(),
            tempRecord.get(index).operation,
            tempRecord.get(index).key,
            tempRecord.get(index).value
        );
        errorRecordHandler.onError(
            new OnRecordErrorException(
                tempRecord.get(index).record,
                Errors.REDIS_03,
                tempRecord.get(index).operation,
                tempRecord.get(index).key,
                tempRecord.get(index).value,
                result.toString()
            )
        );
      }
      index++;
    }
  }

  private void doUpsertString(Record record, List<ErrorRecord> tempRecords, Pipeline pipeline, String key, Field value)
      throws StageException {
    if(value != null && value.getType() == Field.Type.STRING) {
      String val = value.getValueAsString();
      pipeline.set(key, val);
      tempRecords.add(new ErrorRecord(record, "String", key, val));
    } else {
      LOG.error(Errors.REDIS_04.getMessage(), value.getType(), " value should be String");
      errorRecordHandler.onError(
          new OnRecordErrorException(
              record,
              Errors.REDIS_04,
              value.getType(),
              "value should be String"
          )
      );
    }
  }

  private void doUpsertList(Record record, List<ErrorRecord> tempRecords, Pipeline pipeline, String key, Field value)
      throws StageException{
    if(value != null && value.getType() == Field.Type.LIST) {
      List<Field> values = value.getValueAsList();
      for(Field element : values) {
        if(element != null) {
          String val = element.getValueAsString();
          pipeline.lpush(key, val);
          tempRecords.add(new ErrorRecord(record, "List", key, val));
        }
      }
    } else {
      LOG.error(Errors.REDIS_04.getMessage(), value.getType(), "value should be List");
      errorRecordHandler.onError(
          new OnRecordErrorException(
              record,
              Errors.REDIS_04,
              value.getType(),
              "value should be List"
          )
      );
    }
  }

  private void doUpsertSet(Record record, List<ErrorRecord> tempRecords, Pipeline pipeline, String key, Field value)
      throws StageException {
    if(value != null && value.getType() == Field.Type.LIST) {
      List<Field> values = value.getValueAsList();
      for(Field element : values) {
        if(element != null) {
          String val = element.getValueAsString();
          pipeline.sadd(key, val);
          tempRecords.add(new ErrorRecord(record, "Set", key, val));
        }
      }
    } else {
      LOG.error(Errors.REDIS_04.getMessage(), value.getType(), "value should be List");
      errorRecordHandler.onError(
          new OnRecordErrorException(
              record,
              Errors.REDIS_04,
              value.getType(),
              "value should be List"
          )
      );
    }
  }

  private void doUpsertHash(Record record, List<ErrorRecord> tempRecords, Pipeline pipeline, String key, Field value)
      throws StageException {
    if(value != null && value.getType() == Field.Type.MAP) {
      LinkedHashMap<String, Field> values = value.getValueAsListMap();
      for(String field : values.keySet()) {
        if(field != null) {
          String val = values.get(field).getValueAsString();
          pipeline.hset(key, field, val);
          tempRecords.add(new ErrorRecord(record, "Hash", key, val));
        }
      }
    } else {
      LOG.error(Errors.REDIS_04.getMessage(), value.getType(), "value should be Map");
      errorRecordHandler.onError(
          new OnRecordErrorException(
              record,
              Errors.REDIS_04,
              value.getType(),
              "value should be Map"
          )
      );
    }
  }

  private void doPublish(Batch batch) throws StageException {
    Iterator<Record> records = batch.getRecords();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
      while (records.hasNext()) {
        Record record = records.next();
        baos.reset();
        try {
        DataGenerator generator = this.generatorFactory.getGenerator(baos);
        generator.write(record);
        generator.close();

        for (String channel : conf.channel) {
          jedis.publish(channel, baos.toString());
        }
        } catch (JedisConnectionException ex) {
          throw new StageException(Errors.REDIS_06, ex.toString(), ex);
        } catch (IOException ex) {
          LOG.error(Errors.REDIS_04.getMessage(), conf.dataFormat.getLabel(), record);
          errorRecordHandler.onError(
              new OnRecordErrorException(
                  record,
                  Errors.REDIS_04,
                  conf.dataFormat.getLabel(),
                  record
              )
          );
        }
      }
  }
}
