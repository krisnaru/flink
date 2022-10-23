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

import com.google.protobuf.Message;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

public class CfltPbRowDataDeserializationSchema implements DeserializationSchema<RowData> {

  private static final long serialVersionUID = 1L;
  private final String schemaRegistryURL;
  private final RowType rowType;
  private final TypeInformation<RowData> resultTypeInfo;

  private transient KafkaProtobufDeserializer<Message> deserializer;
  private transient ProtoToRowConverter protoToRowConverter;

  public CfltPbRowDataDeserializationSchema(
      String schemaRegistryURL,
      RowType rowType,
      TypeInformation<RowData> resultTypeInfo) {
    Preconditions.checkNotNull(rowType, "rowType cannot be null");
    Preconditions.checkNotNull(schemaRegistryURL, "schemaRegistryURL cannot be null");
    Preconditions.checkArgument(schemaRegistryURL.isEmpty(), "schemaRegistryURL is empty");
    this.schemaRegistryURL = schemaRegistryURL;
    this.rowType = rowType;
    this.resultTypeInfo = resultTypeInfo;
  }

  public void open(DeserializationSchema.InitializationContext context) throws Exception {
    SchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient(Arrays.asList(schemaRegistryURL), 0,
        Arrays.asList(new ProtobufSchemaProvider()),
        null);
    this.deserializer = new KafkaProtobufDeserializer<>(schemaRegistryClient);
    this.protoToRowConverter = new ProtoToRowConverter(this.rowType);
  }

  public RowData deserialize(byte[] message) throws IOException {
    Message protoObject = this.deserializer.deserialize("", message);
    return this.protoToRowConverter.convertProtoBinaryToRow(protoObject);
  }

  public boolean isEndOfStream(RowData nextElement) {
    return false;
  }

  public TypeInformation<RowData> getProducedType() {
    return this.resultTypeInfo;
  }

  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (o != null && this.getClass() == o.getClass()) {
      CfltPbRowDataDeserializationSchema that = (CfltPbRowDataDeserializationSchema) o;
      return Objects.equals(this.schemaRegistryURL, that.schemaRegistryURL) && Objects.equals(this.rowType,
          that.rowType) && Objects.equals(this.resultTypeInfo, that.resultTypeInfo);
    } else {
      return false;
    }
  }

  public int hashCode() {
    return Objects.hash(new Object[]{this.rowType, this.resultTypeInfo});
  }
}

