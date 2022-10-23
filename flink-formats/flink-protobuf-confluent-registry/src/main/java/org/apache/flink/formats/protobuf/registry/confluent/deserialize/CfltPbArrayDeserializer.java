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
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.List;

public class CfltPbArrayDeserializer implements CfltPbDeserializer {

  private final Descriptors.FieldDescriptor fd;
  private final LogicalType elementType;

  private transient CfltPbDeserializer deserializer;

  public CfltPbArrayDeserializer(FieldDescriptor fd, LogicalType elementType) {
    this.fd = fd;
    this.elementType = elementType;
    this.deserializer = CfltPbDeserializeFactory.getProtobufDes(fd, elementType);
  }

  @Override
  public Object deserialize(Object message) {
    List list = (java.util.List) message;
    Object[] objects = new Object[list.size()];
    for (int i = 0; i < list.size(); ++i) {
      Object obj = deserializer.deserialize(list.get(i));
      objects[i] = obj;
    }

    GenericArrayData result = new GenericArrayData(objects);
    return result;
  }
}
