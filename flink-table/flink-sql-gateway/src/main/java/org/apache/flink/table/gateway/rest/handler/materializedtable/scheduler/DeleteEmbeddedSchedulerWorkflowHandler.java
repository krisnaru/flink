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

package org.apache.flink.table.gateway.rest.handler.materializedtable.scheduler;

import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyResponseBody;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.table.gateway.api.SqlGatewayService;
import org.apache.flink.table.gateway.rest.handler.AbstractSqlGatewayRestHandler;
import org.apache.flink.table.gateway.rest.message.materializedtable.scheduler.EmbeddedSchedulerWorkflowRequestBody;
import org.apache.flink.table.gateway.rest.util.SqlGatewayRestAPIVersion;
import org.apache.flink.table.gateway.workflow.scheduler.EmbeddedQuartzScheduler;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/** Handler to delete workflow in embedded scheduler. */
public class DeleteEmbeddedSchedulerWorkflowHandler
        extends AbstractSqlGatewayRestHandler<
                EmbeddedSchedulerWorkflowRequestBody, EmptyResponseBody, EmptyMessageParameters> {

    private final EmbeddedQuartzScheduler quartzScheduler;

    public DeleteEmbeddedSchedulerWorkflowHandler(
            SqlGatewayService service,
            EmbeddedQuartzScheduler quartzScheduler,
            Map<String, String> responseHeaders,
            MessageHeaders<
                            EmbeddedSchedulerWorkflowRequestBody,
                            EmptyResponseBody,
                            EmptyMessageParameters>
                    messageHeaders) {
        super(service, responseHeaders, messageHeaders);
        this.quartzScheduler = quartzScheduler;
    }

    @Override
    protected CompletableFuture<EmptyResponseBody> handleRequest(
            @Nullable SqlGatewayRestAPIVersion version,
            @Nonnull HandlerRequest<EmbeddedSchedulerWorkflowRequestBody> request)
            throws RestHandlerException {
        String workflowName = request.getRequestBody().getWorkflowName();
        String workflowGroup = request.getRequestBody().getWorkflowGroup();
        try {
            quartzScheduler.deleteScheduleWorkflow(workflowName, workflowGroup);
            return CompletableFuture.completedFuture(EmptyResponseBody.getInstance());
        } catch (Exception e) {
            throw new RestHandlerException(
                    e.getMessage(), HttpResponseStatus.INTERNAL_SERVER_ERROR, e);
        }
    }
}
