/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.validation;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

// we are using the annotation for reference purposes only.
// the annotation processor does not work on this maven project
// we have a hardcoded 'datacollector-resource-bundles.json' file in resources
@GenerateResourceBundle
public enum ValidationError implements ErrorCode {
  VALIDATION_0000("Unsupported pipeline schema '{}'"),

  VALIDATION_0001("The pipeline is empty"),
  VALIDATION_0002("The pipeline includes unconnected stages. Cannot reach the following stages: {}"),
  VALIDATION_0003("The first stage must be an origin"),
  VALIDATION_0004("This stage cannot be an origin"),
  VALIDATION_0005("Stage name already defined"),
  VALIDATION_0006("Stage definition does not exist, library '{}', name '{}', version '{}'"),
  VALIDATION_0007("Configuration value is required"),
  VALIDATION_0008("Invalid configuration"),
  VALIDATION_0009("Configuration should be a '{}'"),
  VALIDATION_0010("Output streams '{}' are already defined by stage '{}'"),
  VALIDATION_0011("Stage has open output streams"),
    // Issues are augmented with an array of the open streams in the additionalInfo map property

  VALIDATION_0012("{} cannot have input streams '{}'"),
  VALIDATION_0013("{} cannot have output streams '{}'"),
  VALIDATION_0014("{} must have input streams"),
  VALIDATION_0015("Stage must have '{}' output stream(s) but has '{}'"),
  VALIDATION_0016("Invalid stage name. Names can include the following characters '{}'"),
  VALIDATION_0017("Invalid input stream names '{}'. Streams can include the following characters '{}'"),
  VALIDATION_0018("Invalid output stream names '{}'. Streams can include the following characters '{}'"),

  VALIDATION_0019("Stream condition at index '{}' is not a map"),
  VALIDATION_0020("Stream condition at index '{}' must have a '{}' entry"),
  VALIDATION_0021("Stream condition at index '{}' entry '{}' cannot be NULL"),
  VALIDATION_0022("Stream condition at index '{}' entry '{}' must be a string"),
  VALIDATION_0023("Stream condition at index '{}' entry '{}' cannot be empty"),

  VALIDATION_0024("Configuration of type Map expressed as a list of key/value pairs cannot have null elements in the " +
                  "list. The element at index '{}' is NULL."),
  VALIDATION_0025("Configuration of type Map expressed as a list of key/value pairs must have the 'key' and 'value'" +
                  "entries in the List's Map elements. The element at index '{}' is does not have those 2 entries"),
  VALIDATION_0026("Configuration of type Map expressed as List of key/value pairs must have Map entries in the List," +
                  "element at index '{}' has '{}'"),

  VALIDATION_0027("Data rule '{}' refers to a stream '{}' which is not found in the pipeline configuration"),
  VALIDATION_0028("Data rule '{}' refers to a stage '{}' which is not found in the pipeline configuration"),

  VALIDATION_0029("Configuration must be a string, instead of a '{}'"),
  VALIDATION_0030("The expression value '{}' must be within '${...}'"),

  VALIDATION_0031("The property '{}' should be a single character"),
  VALIDATION_0032("Stage must have at least one output stream"),

  //Rule Validation Errors
  VALIDATION_0040("The data rule property '{}' must be defined"),
  VALIDATION_0041("The Sampling Percentage property must have a value between 0 and 100"),
  VALIDATION_0042("Email alert is enabled, but no email is specified"),
  VALIDATION_0043("The value defined for Threshold Value is not a number"),
  VALIDATION_0044("The Threshold Value property must have a value between 0 and 100"),
  VALIDATION_0045("The condition '{}' defined for the data rule is not valid"),
  VALIDATION_0046("The condition must use the following format: '${value()<operator><number>}'"),
  VALIDATION_0047("The condition '{}' defined for the metric alert is not valid"),
  VALIDATION_0050("The property '{}' must be defined for the metric alert"),


  VALIDATION_0060("Define the error record handling for the pipeline"),
  VALIDATION_0061("Define the directory for error record files"),

  ;

  private final String msg;

  ValidationError(String msg) {
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