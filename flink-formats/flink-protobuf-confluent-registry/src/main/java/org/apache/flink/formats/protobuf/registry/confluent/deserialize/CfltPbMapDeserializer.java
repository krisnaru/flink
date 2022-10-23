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
import com.google.protobuf.Message;
import org.apache.flink.formats.protobuf.PbConstant;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.types.logical.MapType;

import java.util.HashMap;

public class CfltPbMapDeserializer implements CfltPbDeserializer {

  private final Descriptors.FieldDescriptor fd;
  private final MapType mapType;

  private transient FieldDescriptor keyFd;
  private transient FieldDescriptor valueFd;
  private transient CfltPbDeserializer keyDes;

  private transient CfltPbDeserializer valueDes;

  public CfltPbMapDeserializer(FieldDescriptor fd, MapType mapType) {
    this.fd = fd;
    this.mapType = mapType;
    this.keyFd = fd.getMessageType().findFieldByName(
        PbConstant.PB_MAP_KEY_NAME);
    this.valueFd = fd.getMessageType().findFieldByName(PbConstant.PB_MAP_VALUE_NAME);
    this.keyDes = CfltPbDeserializeFactory.getProtobufDes(keyFd, mapType.getKeyType());
    this.valueDes = CfltPbDeserializeFactory.getProtobufDes(valueFd
        , mapType.getValueType());

  }

  @Override
  public Object deserialize(Object message) {
    java.util.Map map = new HashMap();
    for (Object msg : ((java.util.List) message).toArray()) {
      map.put(keyDes.deserialize(((Message) msg).getField(keyFd)),
          valueDes.deserialize(((Message) msg).getField(valueFd)));
    }
    return new GenericMapData(map);
  }
}
