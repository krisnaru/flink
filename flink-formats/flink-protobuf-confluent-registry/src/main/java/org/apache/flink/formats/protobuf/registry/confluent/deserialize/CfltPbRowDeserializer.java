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

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

public class CfltPbRowDeserializer implements CfltPbDeserializer {

  private final Descriptor descriptor;
  private final RowType rowType;

  public CfltPbRowDeserializer(Descriptor descriptor, RowType rowType) {
    this.descriptor = descriptor;
    this.rowType = rowType;
  }

  @Override
  public Object deserialize(Object message) {
    GenericRowData rowData = new GenericRowData(this.rowType.getFieldCount());
    int idx = 0;
    for (String fieldName : rowType.getFieldNames()) {
      LogicalType subType = rowType.getTypeAt(rowType.getFieldIndex(fieldName));
      FieldDescriptor elementFd = this.descriptor.findFieldByName(fieldName);
      CfltPbDeserializer deserializer = CfltPbDeserializeFactory.getProtobufDes(elementFd,
          subType);
      rowData.setField(idx++, deserializer.deserialize(((Message) message).getField(elementFd)));
    }
    return rowData;
  }
}
