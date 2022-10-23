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
import org.apache.flink.formats.protobuf.util.PbFormatUtils;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;

/**
 * Codegen factory class which return {@link CfltPbDeserializer} of different data type.
 */
public class CfltPbDeserializeFactory {

  public static CfltPbDeserializer getProtobufDes(
      Descriptors.FieldDescriptor fd, LogicalType type) {
    // We do not use FieldDescriptor to check because there's no way to get
    // element field descriptor of array type.
    if (type instanceof RowType) {
      return new CfltPbRowDeserializer(fd.getMessageType(), (RowType) type);
    } else if (PbFormatUtils.isSimpleType(type)) {
      return new CfltPbSimpleDeserializer(fd, type);
    } else if (type instanceof ArrayType) {
      return new CfltPbArrayDeserializer(
          fd, ((ArrayType) type).getElementType());
    } else if (type instanceof MapType) {
      return new CfltPbMapDeserializer(fd, (MapType) type);
    } else {
      throw new RuntimeException("Do not support flink type: " + type);
    }
  }

  public static CfltPbDeserializer getProtobufTopRowDes(
      Descriptors.Descriptor descriptor, RowType rowType) {
    return new CfltPbRowDeserializer(descriptor, rowType);
  }
}