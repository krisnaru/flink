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

package org.apache.flink.state.api;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.state.api.functions.KeyedStateBootstrapFunction;
import org.apache.flink.state.api.runtime.SavepointLoader;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** IT test for modifying UIDs in savepoints. */
public class SavepointWriterUidModificationITCase {

    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER_RESOURCE =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(4)
                            .build());

    private static final Collection<Integer> STATE_1 = Arrays.asList(1, 2, 3);
    private static final Collection<Integer> STATE_2 = Arrays.asList(4, 5, 6);

    private static final ValueStateDescriptor<Integer> STATE_DESCRIPTOR =
            new ValueStateDescriptor<>("number", Types.INT);

    @Test
    public void testAddUid(@TempDir Path tmp) throws Exception {
        final String uidHash = new AbstractID().toHexString();
        final String uid = "uid";
        final String originalSavepoint =
                bootstrapState(
                        tmp,
                        (env, writer) ->
                                writer.withOperator(
                                        OperatorIdentifier.forUidHash(uidHash),
                                        bootstrap(env, STATE_1)));
        final String newSavepoint =
                modifySavepoint(
                        tmp,
                        originalSavepoint,
                        writer ->
                                writer.changeOperatorIdentifier(
                                        OperatorIdentifier.forUidHash(uidHash),
                                        OperatorIdentifier.forUid(uid)));

        runAndValidate(newSavepoint, ValidationParameters.of(STATE_1, uid, null));
    }

    @Test
    public void testChangeUid(@TempDir Path tmp) throws Exception {
        final String uid = "uid";
        final String newUid = "fabulous";
        final String originalSavepoint =
                bootstrapState(
                        tmp,
                        (env, writer) ->
                                writer.withOperator(
                                        OperatorIdentifier.forUid(uid), bootstrap(env, STATE_1)));
        final String newSavepoint =
                modifySavepoint(
                        tmp,
                        originalSavepoint,
                        writer ->
                                writer.changeOperatorIdentifier(
                                        OperatorIdentifier.forUid(uid),
                                        OperatorIdentifier.forUid(newUid)));

        runAndValidate(newSavepoint, ValidationParameters.of(STATE_1, newUid, null));
    }

    @Test
    public void testChangeUidHashOnly(@TempDir Path tmp) throws Exception {
        final String uid = "uid";
        final String newUidHash = new AbstractID().toHexString();
        final String originalSavepoint =
                bootstrapState(
                        tmp,
                        (env, writer) ->
                                writer.withOperator(
                                        OperatorIdentifier.forUid(uid), bootstrap(env, STATE_1)));
        final String newSavepoint =
                modifySavepoint(
                        tmp,
                        originalSavepoint,
                        writer ->
                                writer.changeOperatorIdentifier(
                                        OperatorIdentifier.forUid(uid),
                                        OperatorIdentifier.forUidHash(newUidHash)));

        runAndValidate(newSavepoint, ValidationParameters.of(STATE_1, null, newUidHash));
    }

    @Test
    public void testSwapUid(@TempDir Path tmp) throws Exception {
        final String uid1 = "uid1";
        final String uid2 = "uid2";
        final String originalSavepoint =
                bootstrapState(
                        tmp,
                        (env, writer) ->
                                writer.withOperator(
                                                OperatorIdentifier.forUid(uid1),
                                                bootstrap(env, STATE_1))
                                        .withOperator(
                                                OperatorIdentifier.forUid(uid2),
                                                bootstrap(env, STATE_2)));
        final String newSavepoint =
                modifySavepoint(
                        tmp,
                        originalSavepoint,
                        writer ->
                                writer.changeOperatorIdentifier(
                                                OperatorIdentifier.forUid(uid1),
                                                OperatorIdentifier.forUid(uid2))
                                        .changeOperatorIdentifier(
                                                OperatorIdentifier.forUid(uid2),
                                                OperatorIdentifier.forUid(uid1)));

        runAndValidate(
                newSavepoint,
                ValidationParameters.of(STATE_1, uid2, null),
                ValidationParameters.of(STATE_2, uid1, null));
    }

    private static String bootstrapState(
            Path tmp, BiConsumer<StreamExecutionEnvironment, SavepointWriter> mutator)
            throws Exception {
        final String savepointPath =
                tmp.resolve(new AbstractID().toHexString()).toAbsolutePath().toString();

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        final SavepointWriter writer = SavepointWriter.newSavepoint(env, 128);

        mutator.accept(env, writer);

        writer.write(savepointPath);

        env.execute("Bootstrap");

        return savepointPath;
    }

    private static StateBootstrapTransformation<Integer> bootstrap(
            StreamExecutionEnvironment env, Collection<Integer> data) {
        return OperatorTransformation.bootstrapWith(env.fromData(data))
                .keyBy(v -> v)
                .transform(new StateBootstrapper());
    }

    private static String modifySavepoint(
            Path tmp, String savepointPath, Consumer<SavepointWriter> mutator) throws Exception {
        final String newSavepointPath =
                tmp.resolve(new AbstractID().toHexString()).toAbsolutePath().toString();

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        SavepointWriter writer = SavepointWriter.fromExistingSavepoint(env, savepointPath);

        mutator.accept(writer);
        writer.write(newSavepointPath);

        env.execute("Modifying");

        return newSavepointPath;
    }

    private static void runAndValidate(
            String savepointPath, ValidationParameters... validationParameters) throws Exception {
        // validate metadata
        CheckpointMetadata metadata = SavepointLoader.loadSavepointMetadata(savepointPath);
        assertThat(metadata.getOperatorStates().size()).isEqualTo(validationParameters.length);
        for (ValidationParameters validationParameter : validationParameters) {
            if (validationParameter.getUid() != null) {
                Set<OperatorState> operators =
                        metadata.getOperatorStates().stream()
                                .filter(
                                        os ->
                                                os.getOperatorUid().isPresent()
                                                        && os.getOperatorUid()
                                                                .get()
                                                                .equals(
                                                                        validationParameter
                                                                                .getUid()))
                                .collect(Collectors.toSet());
                assertThat(operators.size()).isEqualTo(1);
                assertThat(operators.iterator().next().getOperatorID())
                        .isEqualTo(
                                OperatorIdentifier.forUid(validationParameter.getUid())
                                        .getOperatorId());
            } else {
                Set<OperatorState> operators =
                        metadata.getOperatorStates().stream()
                                .filter(
                                        os ->
                                                os.getOperatorID()
                                                        .toHexString()
                                                        .equals(validationParameter.getUidHash()))
                                .collect(Collectors.toSet());
                assertThat(operators.size()).isEqualTo(1);
                assertThat(operators.iterator().next().getOperatorUid()).isEmpty();
            }
        }

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // prepare collection of state
        final List<CloseableIterator<Integer>> iterators = new ArrayList<>();
        for (ValidationParameters validationParameter : validationParameters) {
            SingleOutputStreamOperator<Integer> stream =
                    env.fromData(validationParameter.getState())
                            .keyBy(v -> v)
                            .map(new StateReader());
            if (validationParameter.getUid() != null) {
                iterators.add(stream.uid(validationParameter.getUid()).collectAsync());
            } else {
                iterators.add(stream.setUidHash(validationParameter.getUidHash()).collectAsync());
            }
        }

        // run job
        StreamGraph streamGraph = env.getStreamGraph();
        streamGraph.setSavepointRestoreSettings(
                SavepointRestoreSettings.forPath(savepointPath, false));
        env.executeAsync(streamGraph);

        // validate state
        for (int i = 0; i < validationParameters.length; i++) {
            assertThat(iterators.get(i))
                    .toIterable()
                    .containsExactlyInAnyOrderElementsOf(validationParameters[i].getState());
        }

        for (CloseableIterator<Integer> iterator : iterators) {
            iterator.close();
        }
    }

    private static class ValidationParameters {
        private final Collection<Integer> state;
        private final String uid;
        private final String uidHash;

        public ValidationParameters(
                final Collection<Integer> state, final String uid, final String uidHash) {
            this.state = state;
            this.uid = uid;
            this.uidHash = uidHash;
        }

        public Collection<Integer> getState() {
            return state;
        }

        public String getUid() {
            return uid;
        }

        public String getUidHash() {
            return uidHash;
        }

        public static ValidationParameters of(
                final Collection<Integer> state, final String uid, final String uidHash) {
            return new ValidationParameters(state, uid, uidHash);
        }
    }

    /** A savepoint writer function. */
    public static class StateBootstrapper extends KeyedStateBootstrapFunction<Integer, Integer> {
        private transient ValueState<Integer> state;

        @Override
        public void open(OpenContext openContext) {
            state = getRuntimeContext().getState(STATE_DESCRIPTOR);
        }

        @Override
        public void processElement(Integer value, Context ctx) throws Exception {
            state.update(value);
        }
    }

    /** A savepoint reader function. */
    public static class StateReader extends RichMapFunction<Integer, Integer> {
        private transient ValueState<Integer> state;

        @Override
        public void open(OpenContext openContext) {
            state = getRuntimeContext().getState(STATE_DESCRIPTOR);
        }

        @Override
        public Integer map(Integer value) throws Exception {
            return state.value();
        }
    }
}
