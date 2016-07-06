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
package com.streamsets.pipeline.stage.lib.hive;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum Errors implements ErrorCode {
  HIVE_00("Cannot have multiple field mappings for the same column: '{}'"),
  HIVE_01("Error: {}"),
  HIVE_02("Schema '{}' does not exist."),
  HIVE_03("Table '{}.{}' does not exist."),
  HIVE_04("Thrift protocol error: {}"),
  HIVE_05("Hive Metastore error: {}"),
  HIVE_06("Configuration file '{}' is missing from '{}'"),
  HIVE_07("Configuration dir '{}' does not exist"),
  HIVE_08("Partition field paths '{}' missing from record"),
  HIVE_09("Hive Streaming Error: {}"),
  HIVE_11("Failed to get login user"),
  HIVE_12("Failed to create Hive Endpoint: {}"),
  HIVE_13("Hive Metastore Thrift URL or Hive Configuration Directory is required."),
  HIVE_14("Hive Metastore Thrift URL {} is not a valid URI"),
  HIVE_15("Hive JDBC Driver {} not present in the class Path"),
  HIVE_16("Unsupported HMS Cache Type: {}"),
  HIVE_17("Information {} missing or invalid in the metadata record: {}"),
  HIVE_18("Error when serializing AVRO schema to HDFS folder location: {}. Reason: {}"),
  HIVE_19("Unsupported Type: {}"),
  HIVE_20("Error executing SQL: {}, Reason:{}"),
  HIVE_21("Type Mismatch, Expected: {}, Actual: {}"),
  HIVE_22("Cannot make connection with default hive database with URL:{}. Reason:{}"),
  HIVE_23("TBL Properties '{}' Mismatch: Actual: {} , Expected: {}"),
  HIVE_24("Type conversion from Hive.{} to Avro Type is not supported"),
  HIVE_25("Trying to create partition for non existing table: {}"),
  HIVE_26("Invalid decimal value {} in field {}: {} {} is more then expected {} "),
  HIVE_27("Partition Information mismatch for the table {}"),
  HIVE_28("Partition Column {} has Type Mismatch in table {}. Expected Type: {}, Actual Type: {}"),
  HIVE_29("Can't calculate {} for field '{}' - expression '{}' evaluated to '{}'"),
  HIVE_30("Invalid column name {}"),
  HIVE_31("Partition Location mismatch. Actual : {}, Expected: {}"),
  HIVE_32("Table {} is created using unsupported Storage Format Type {} "),
  ;
  private final String msg;

  Errors(String msg) {
    this.msg = msg;
  }

  @Override
  public String getCode() {
    return name();
  }

  @Override
  public String getMessage() {
    return msg;
  }
}
