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
package com.streamsets.pipeline.stage.lib.hive;

import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.generator.avro.AvroSchemaGenerator;
import com.streamsets.pipeline.stage.lib.hive.exceptions.HiveStageCheckedException;
import com.streamsets.pipeline.stage.lib.hive.typesupport.HiveTypeInfo;
import com.streamsets.pipeline.stage.lib.hive.typesupport.DecimalHiveTypeSupport.DecimalTypeInfo;
import com.google.common.collect.ImmutableList;
import org.apache.avro.Schema;
import org.codehaus.jackson.node.IntNode;
import org.codehaus.jackson.node.NullNode;

import java.util.LinkedHashMap;
import java.util.Map;

public class AvroHiveSchemaGenerator extends AvroSchemaGenerator<Map<String, HiveTypeInfo>> {

  private static final String LOGICAL_TYPE_PROP = "logicalType";
  private static final String DATE_TYPE = "date";
  private static final String DECIMAL_TYPE = "decimal";
  private static final String PRECISION = "precision";
  private static final String SCALE = "scale";

  public AvroHiveSchemaGenerator(final String name){
    super(name);
  }

  /**
   * It takes a record structure in <String, HiveTypeInfo> format.
   * Generate a schema and return in String.
   * @param record : record structure
   * @return String representation of Avro schema.
   * @throws StageException: If record contains unsupported type
   */
  @Override
  public String inferSchema(Map<String, HiveTypeInfo> record)
      throws StageException
  {
    Map<String, Schema> fields = new LinkedHashMap();
    for(Map.Entry<String, HiveTypeInfo> pair:  record.entrySet()) {
      if(!HiveMetastoreUtil.validateColumnName(pair.getKey())) {
        throw new HiveStageCheckedException(Errors.HIVE_30, pair.getKey());
      }
      Schema columnSchema = Schema.createUnion(ImmutableList.of(Schema.create(Schema.Type.NULL), traverse(pair)));
      // We always set default value to null
      columnSchema.addProp("default", NullNode.getInstance());
      fields.put(pair.getKey(), columnSchema);
    }
    Schema schema =  buildSchema(fields);
    return schema.toString();
  }

  private static Schema traverse(Map.Entry<String, HiveTypeInfo> node)
  throws StageException {
    switch(node.getValue().getHiveType()){
      case STRING:
        return Schema.create(Schema.Type.STRING);

      case BOOLEAN:
        return Schema.create(Schema.Type.BOOLEAN);

      case INT:
        return Schema.create(Schema.Type.INT);

      case BIGINT:
        return Schema.create(Schema.Type.LONG);

      case FLOAT:
        return Schema.create(Schema.Type.FLOAT);

      case DOUBLE:
        return Schema.create(Schema.Type.DOUBLE);

      case BINARY:
        return Schema.create(Schema.Type.BYTES);

      case DATE:
        Schema dateSchema = Schema.create(Schema.Type.INT);
        dateSchema.addProp(LOGICAL_TYPE_PROP, DATE_TYPE);
        return dateSchema;

      case DECIMAL:
        Utils.checkArgument(node.getValue() instanceof DecimalTypeInfo, "Invalid type used in HiveTypeInfo");
        DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo)node.getValue();
        Schema schema = Schema.create(Schema.Type.BYTES);
        schema.addProp(LOGICAL_TYPE_PROP, DECIMAL_TYPE);
        schema.addProp(PRECISION, getJsonNode(decimalTypeInfo.getPrecision()));
        schema.addProp(SCALE, getJsonNode(decimalTypeInfo.getScale()));
        return schema;

      default:
        // Accessing Unsupported Type (Map, Union, Array, Enum, NULL, Record)
        throw new StageException(Errors.HIVE_24, node.getValue().getHiveType());
    }
  }

  static IntNode getJsonNode(int value){
    return new IntNode(value);
  }
}
