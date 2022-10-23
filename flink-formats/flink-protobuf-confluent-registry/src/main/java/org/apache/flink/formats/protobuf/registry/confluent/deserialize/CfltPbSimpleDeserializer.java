/*
 * Licensed to the Apache Software Foundation (ASF) under one
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
package org.apache.flink.formats.protobuf.confluent.deserialize;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

public class CfltPbSimpleDeserializer implements CfltPbDeserializer {

  private final Descriptors.FieldDescriptor fd;
  private final LogicalType logicalType;

  public CfltPbSimpleDeserializer(FieldDescriptor fd, LogicalType logicalType) {
    this.fd = fd;
    this.logicalType = logicalType;
  }

  @Override
  public Object deserialize(Object message) {
    switch (fd.getJavaType()) {
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case BOOLEAN:
        return message;
      case BYTE_STRING:
        return ((String) message).getBytes();
      case STRING:
        return BinaryStringData.fromString(message.toString());
      case ENUM:
        if (logicalType.getTypeRoot() == LogicalTypeRoot.CHAR
            || logicalType.getTypeRoot() == LogicalTypeRoot.VARCHAR) {
          return BinaryStringData.fromString(message.toString());
        } else if (logicalType.getTypeRoot() == LogicalTypeRoot.TINYINT
            || logicalType.getTypeRoot() == LogicalTypeRoot.SMALLINT
            || logicalType.getTypeRoot() == LogicalTypeRoot.INTEGER
            || logicalType.getTypeRoot() == LogicalTypeRoot.BIGINT) {
          return Integer.parseInt(message.toString());
        } else {
          throw new RuntimeException(
              "Illegal type for protobuf enum, only char/vachar/int/bigint is supported");
        }
      default:
        throw new RuntimeException(
            "Unsupported protobuf simple type: " + fd.getJavaType());
    }
  }
}
