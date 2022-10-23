/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
class CfltPbFormatFactoryTest {
    
    private static final RowType ROW_TYPE =
            (RowType) SCHEMA.toPhysicalRowDataType().getLogicalType();

    private static final String SUBJECT = "test-subject";
    private static final String REGISTRY_URL = "http://localhost:8081";

    private static final Map<String, String> EXPECTED_OPTIONAL_PROPERTIES = new HashMap<>();

    static {
        EXPECTED_OPTIONAL_PROPERTIES.put(
                "schema.registry.ssl.keystore.location", getAbsolutePath("/test-keystore.jks"));
        EXPECTED_OPTIONAL_PROPERTIES.put("schema.registry.ssl.keystore.password", "123456");
        EXPECTED_OPTIONAL_PROPERTIES.put(
                "schema.registry.ssl.truststore.location", getAbsolutePath("/test-keystore.jks"));
        EXPECTED_OPTIONAL_PROPERTIES.put("schema.registry.ssl.truststore.password", "123456");
        EXPECTED_OPTIONAL_PROPERTIES.put("basic.auth.credentials.source", "USER_INFO");
        EXPECTED_OPTIONAL_PROPERTIES.put("basic.auth.user.info", "user:pwd");
        EXPECTED_OPTIONAL_PROPERTIES.put("bearer.auth.token", "CUSTOM");
    }
    
    @Test
    void testDeserializationSchema() {
        
    }

    @Test
    void testSerializationSchema() {
    }
}
