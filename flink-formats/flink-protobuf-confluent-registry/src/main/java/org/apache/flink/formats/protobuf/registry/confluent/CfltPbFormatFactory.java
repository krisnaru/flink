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
package org.apache.flink.formats.protobuf.confluent;

import static com.roku.common.flink.formats.protobuf.confluent.CfltPbFormatOptions.BASIC_AUTH_CREDENTIALS_SOURCE;
import static com.roku.common.flink.formats.protobuf.confluent.CfltPbFormatOptions.BASIC_AUTH_USER_INFO;
import static com.roku.common.flink.formats.protobuf.confluent.CfltPbFormatOptions.BEARER_AUTH_CREDENTIALS_SOURCE;
import static com.roku.common.flink.formats.protobuf.confluent.CfltPbFormatOptions.BEARER_AUTH_TOKEN;
import static com.roku.common.flink.formats.protobuf.confluent.CfltPbFormatOptions.PROPERTIES;
import static com.roku.common.flink.formats.protobuf.confluent.CfltPbFormatOptions.SSL_KEYSTORE_LOCATION;
import static com.roku.common.flink.formats.protobuf.confluent.CfltPbFormatOptions.SSL_KEYSTORE_PASSWORD;
import static com.roku.common.flink.formats.protobuf.confluent.CfltPbFormatOptions.SSL_TRUSTSTORE_LOCATION;
import static com.roku.common.flink.formats.protobuf.confluent.CfltPbFormatOptions.SSL_TRUSTSTORE_PASSWORD;
import static com.roku.common.flink.formats.protobuf.confluent.CfltPbFormatOptions.SUBJECT;
import static com.roku.common.flink.formats.protobuf.confluent.CfltPbFormatOptions.TOPIC;
import static com.roku.common.flink.formats.protobuf.confluent.CfltPbFormatOptions.URL;

import com.roku.common.flink.formats.protobuf.confluent.deserialize.CfltPbRowDataDeserializationSchema;
import com.roku.common.flink.formats.protobuf.confluent.serialize.CfltPbRowDataSerializationSchema;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.formats.protobuf.PbFormatConfig;
import org.apache.flink.formats.protobuf.PbFormatOptions;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.format.ProjectableDecodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;

/**
 * Table format factory for providing configured instances of Schema Registry Protobuf to RowData
 * {@link SerializationSchema} and {@link DeserializationSchema}.
 */
@Internal
public class CfltPbFormatFactory
    implements DeserializationFormatFactory, SerializationFormatFactory {

  public static final String IDENTIFIER = "protobuf-confluent";

  @Override
  public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(
      DynamicTableFactory.Context context, ReadableConfig formatOptions) {
    FactoryUtil.validateFactoryOptions(this, formatOptions);

    String schemaRegistryURL = formatOptions.get(URL);
    return new ProjectableDecodingFormat<DeserializationSchema<RowData>>() {
      @Override
      public DeserializationSchema<RowData> createRuntimeDecoder(
          DynamicTableSource.Context context,
          DataType producedDataType,
          int[][] projections) {
        producedDataType = Projection.of(projections).project(producedDataType);
        final RowType rowType = (RowType) producedDataType.getLogicalType();
        final TypeInformation<RowData> rowDataTypeInfo =
            context.createTypeInformation(producedDataType);
        return new CfltPbRowDataDeserializationSchema(
            schemaRegistryURL,
            rowType,
            rowDataTypeInfo
        );
      }

      @Override
      public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
      }
    };
  }

  @Override
  public EncodingFormat<SerializationSchema<RowData>> createEncodingFormat(
      DynamicTableFactory.Context context, ReadableConfig formatOptions) {
    FactoryUtil.validateFactoryOptions(this, formatOptions);

    String schemaRegistryURL = formatOptions.get(URL);
    Optional<String> subject = formatOptions.getOptional(SUBJECT);
    Optional<String> topic = formatOptions.getOptional(TOPIC);
    PbFormatConfig formatConfig = buildConfig(formatOptions);

    if (!subject.isPresent()) {
      throw new ValidationException(
          String.format(
              "Option %s.%s is required for serialization",
              IDENTIFIER, SUBJECT.key()));
    }

    if (!topic.isPresent()) {
      throw new ValidationException(
          String.format(
              "Option %s.%s is required for serialization",
              IDENTIFIER, TOPIC.key()));
    }

    return new EncodingFormat<SerializationSchema<RowData>>() {
      @Override
      public SerializationSchema<RowData> createRuntimeEncoder(
          DynamicTableSink.Context context, DataType consumedDataType) {
        final RowType rowType = (RowType) consumedDataType.getLogicalType();
        return new CfltPbRowDataSerializationSchema(
            schemaRegistryURL,
            topic.get(),
            rowType,
            formatConfig
        );
      }

      @Override
      public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
      }
    };
  }

  @Override
  public String factoryIdentifier() {
    return IDENTIFIER;
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    Set<ConfigOption<?>> options = new HashSet<>();
    options.add(URL);
    return options;
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    Set<ConfigOption<?>> options = new HashSet<>();
    options.add(SUBJECT);
    options.add(PROPERTIES);
    options.add(SSL_KEYSTORE_LOCATION);
    options.add(SSL_KEYSTORE_PASSWORD);
    options.add(SSL_TRUSTSTORE_LOCATION);
    options.add(SSL_TRUSTSTORE_PASSWORD);
    options.add(BASIC_AUTH_CREDENTIALS_SOURCE);
    options.add(BASIC_AUTH_USER_INFO);
    options.add(BEARER_AUTH_CREDENTIALS_SOURCE);
    options.add(BEARER_AUTH_TOKEN);
    return options;
  }

  @Override
  public Set<ConfigOption<?>> forwardOptions() {
    return Stream.of(
            URL,
            SUBJECT,
            PROPERTIES,
            SSL_KEYSTORE_LOCATION,
            SSL_KEYSTORE_PASSWORD,
            SSL_TRUSTSTORE_LOCATION,
            SSL_TRUSTSTORE_PASSWORD,
            BASIC_AUTH_CREDENTIALS_SOURCE,
            BASIC_AUTH_USER_INFO,
            BEARER_AUTH_CREDENTIALS_SOURCE,
            BEARER_AUTH_TOKEN)
        .collect(Collectors.toSet());
  }

  public static @Nullable Map<String, String> buildOptionalPropertiesMap(
      ReadableConfig formatOptions) {
    final Map<String, String> properties = new HashMap<>();

    formatOptions.getOptional(PROPERTIES).ifPresent(properties::putAll);

    formatOptions
        .getOptional(SSL_KEYSTORE_LOCATION)
        .ifPresent(v -> properties.put("schema.registry.ssl.keystore.location", v));
    formatOptions
        .getOptional(SSL_KEYSTORE_PASSWORD)
        .ifPresent(v -> properties.put("schema.registry.ssl.keystore.password", v));
    formatOptions
        .getOptional(SSL_TRUSTSTORE_LOCATION)
        .ifPresent(v -> properties.put("schema.registry.ssl.truststore.location", v));
    formatOptions
        .getOptional(SSL_TRUSTSTORE_PASSWORD)
        .ifPresent(v -> properties.put("schema.registry.ssl.truststore.password", v));
    formatOptions
        .getOptional(BASIC_AUTH_CREDENTIALS_SOURCE)
        .ifPresent(v -> properties.put("basic.auth.credentials.source", v));
    formatOptions
        .getOptional(BASIC_AUTH_USER_INFO)
        .ifPresent(v -> properties.put("basic.auth.user.info", v));
    formatOptions
        .getOptional(BEARER_AUTH_CREDENTIALS_SOURCE)
        .ifPresent(v -> properties.put("bearer.auth.credentials.source", v));
    formatOptions
        .getOptional(BEARER_AUTH_TOKEN)
        .ifPresent(v -> properties.put("bearer.auth.token", v));

    if (properties.isEmpty()) {
      return null;
    }
    return properties;
  }

  private static PbFormatConfig buildConfig(ReadableConfig formatOptions) {
    PbFormatConfig.PbFormatConfigBuilder configBuilder =
        new PbFormatConfig.PbFormatConfigBuilder();
    configBuilder.messageClassName(formatOptions.get(PbFormatOptions.MESSAGE_CLASS_NAME));
    formatOptions
        .getOptional(PbFormatOptions.IGNORE_PARSE_ERRORS)
        .ifPresent(configBuilder::ignoreParseErrors);
    formatOptions
        .getOptional(PbFormatOptions.READ_DEFAULT_VALUES)
        .ifPresent(configBuilder::readDefaultValues);
    formatOptions
        .getOptional(PbFormatOptions.WRITE_NULL_STRING_LITERAL)
        .ifPresent(configBuilder::writeNullStringLiterals);
    return configBuilder.build();
  }
}
