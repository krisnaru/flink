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
package org.apache.flink.formats.protobuf.confluent.serialize;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.formats.protobuf.PbFormatConfig;
import org.apache.flink.formats.protobuf.util.PbFormatUtils;
import org.apache.flink.formats.protobuf.util.PbSchemaValidationUtils;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import java.util.Arrays;

/**
 * Serialization schema from Flink to Protobuf types.
 *
 * <p>Serializes a {@link RowData } to protobuf binary data.
 *
 * <p>Failures during deserialization are forwarded as wrapped {@link FlinkRuntimeException}.
 */
public class CfltPbRowDataSerializationSchema implements SerializationSchema<RowData> {

  public static final long serialVersionUID = 1L;

  private final RowType rowType;
  private final PbFormatConfig pbFormatConfig;
  private transient RowToProtoConverter rowToProtoConverter;
  private final String schemaRegistryURL;
  private final String topic;
  private transient KafkaProtobufSerializer serializer;

  public CfltPbRowDataSerializationSchema(String schemaRegistryURL,
      String topic, RowType rowType, PbFormatConfig pbFormatConfig) {
    Preconditions.checkNotNull(rowType, "rowType cannot be null");
    Preconditions.checkArgument(schemaRegistryURL == null || schemaRegistryURL.isEmpty(),
        "schemaRegistryURL is null or empty");
    Preconditions.checkArgument(topic == null || topic.isEmpty(),
        "topic is null or empty");
    this.schemaRegistryURL = schemaRegistryURL;
    this.topic = topic;
    this.rowType = rowType;
    this.pbFormatConfig = pbFormatConfig;
    Descriptors.Descriptor descriptor =
        PbFormatUtils.getDescriptor(pbFormatConfig.getMessageClassName());
    PbSchemaValidationUtils.validate(descriptor, rowType);
  }

  @Override
  public void open(InitializationContext context) throws Exception {
    rowToProtoConverter = new RowToProtoConverter(rowType, pbFormatConfig);
    SchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient(Arrays.asList(schemaRegistryURL), 0,
        Arrays.asList(new ProtobufSchemaProvider()),
        null);
    this.serializer = new KafkaProtobufSerializer(schemaRegistryClient);
  }

  @Override
  public byte[] serialize(RowData rowData) {
    try {
      Message message = rowToProtoConverter.convertRowToProtoBinary(rowData);
      return this.serializer.serialize(topic, message);
    } catch (Exception e) {
      throw new FlinkRuntimeException(e);
    }
  }
}