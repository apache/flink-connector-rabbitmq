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

package org.apache.flink.connector.rabbitmq.source;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.MiniClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.rabbitmq.common.ConsistencyMode;
import org.apache.flink.connector.rabbitmq.common.RabbitMQConnectionConfig;
import org.apache.flink.connector.rabbitmq.common.RabbitMQContainerClient;
import org.apache.flink.core.testutils.AllCallbackWrapper;
import org.apache.flink.core.testutils.TestContainerExtension;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.test.junit5.InjectClusterClient;
import org.apache.flink.test.junit5.MiniClusterExtension;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration.MINIMAL_CHECKPOINT_TIME;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The tests for the RabbitMQ source with different consistency modes. As the tests are working a
 * lot with timeouts to uphold stream it is possible that tests might fail.
 */
class RabbitMQSourceITest {
    static final Logger LOG = LoggerFactory.getLogger(RabbitMQSourceITest.class);
    private static final List<String> sinkedMessages =
            Collections.synchronizedList(new ArrayList<>());
    private static CountDownLatch messageLatch;
    private static CountDownLatch checkpointLatch;
    private static int failAtNthMessage = 0;
    private static final int RABBITMQ_PORT = 5672;
    private static RabbitMQContainerClient<String> client;

    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER_EXTENSION =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(4)
                            .setNumberSlotsPerTaskManager(4)
                            .setConfiguration(new Configuration())
                            .build());

    @RegisterExtension
    static AllCallbackWrapper<TestContainerExtension<RabbitMQContainer>> rabbitMqCallbackWrapper =
            new AllCallbackWrapper<>(
                    new TestContainerExtension<>(
                            () ->
                                    new RabbitMQContainer(
                                                    DockerImageName.parse("rabbitmq")
                                                            .withTag("3.7.25-management-alpine"))
                                            .withExposedPorts(RABBITMQ_PORT)
                                            .withLogConsumer(new Slf4jLogConsumer(LOG))
                                            .waitingFor(Wait.forListeningPort())));

    @BeforeEach
    void setUpContainerClient() {
        client =
                new RabbitMQContainerClient<>(
                        rabbitMqCallbackWrapper.getCustomExtension().getTestContainer());
        sinkedMessages.clear();
        failAtNthMessage = -1;
        messageLatch = null;
    }

    protected void sendToRabbit(List<String> messages) throws IOException {
        client.sendMessages(new SimpleStringSchema(), messages);
    }

    protected void sendToRabbit(List<String> messages, List<String> correlationIds)
            throws IOException {
        for (int i = 0; i < messages.size(); i++) {
            client.sendMessage(new SimpleStringSchema(), messages.get(i), correlationIds.get(i));
        }
    }

    protected List<String> getRandomMessages(int numberOfMessages) {
        List<String> messages = new ArrayList<>();
        for (int i = 0; i < numberOfMessages; i++) {
            messages.add(UUID.randomUUID().toString());
        }
        return messages;
    }

    protected List<String> getSequentialMessages(int numberOfMessages) {
        List<String> messages = new ArrayList<>();
        for (int i = 0; i < numberOfMessages; i++) {
            messages.add("Message " + i);
        }
        return messages;
    }

    protected DataStream<String> addSourceOn(
            StreamExecutionEnvironment env, ConsistencyMode consistencyMode)
            throws IOException, TimeoutException {
        String queueName = client.createQueue(false);

        final RabbitMQConnectionConfig connectionConfig =
                new RabbitMQConnectionConfig.Builder()
                        .setHost(
                                rabbitMqCallbackWrapper
                                        .getCustomExtension()
                                        .getTestContainer()
                                        .getHost())
                        .setVirtualHost("/")
                        .setUserName(
                                rabbitMqCallbackWrapper
                                        .getCustomExtension()
                                        .getTestContainer()
                                        .getAdminUsername())
                        .setPassword(
                                rabbitMqCallbackWrapper
                                        .getCustomExtension()
                                        .getTestContainer()
                                        .getAdminPassword())
                        .setPort(
                                rabbitMqCallbackWrapper
                                        .getCustomExtension()
                                        .getTestContainer()
                                        .getMappedPort(RABBITMQ_PORT))
                        .build();

        RabbitMQSource<String> rabbitMQSource =
                RabbitMQSource.<String>builder()
                        .setConnectionConfig(connectionConfig)
                        .setQueueName(queueName)
                        .setDeserializationSchema(new SimpleStringSchema())
                        .setConsistencyMode(consistencyMode)
                        .build();

        return env.fromSource(rabbitMQSource, WatermarkStrategy.noWatermarks(), "RabbitMQSource")
                .setParallelism(1);
    }

    /** CollectSink to access the messages from the stream. */
    private static class CollectSink implements SinkFunction<String>, CheckpointListener {

        public static void addOnStream(DataStream<String> stream) {
            stream.addSink(new CollectSink()).setParallelism(1);
        }

        @Override
        public void invoke(String value, Context context) throws Exception {

            if (failAtNthMessage > 0) {
                failAtNthMessage -= 1;
                if (failAtNthMessage == 0) {
                    throw new Exception("This is supposed to be thrown.");
                }
            }
            sinkedMessages.add(value);
            messageLatch.countDown();
        }

        @Override
        public void notifyCheckpointComplete(long l) {
            if (checkpointLatch != null) {
                checkpointLatch.countDown();
            }
        }
    }

    private StreamExecutionEnvironment getEnv(CheckpointingMode checkpointingMode) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(MINIMAL_CHECKPOINT_TIME, checkpointingMode);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, 1000));
        return env;
    }

    // --------------- at most once ---------------
    @Test
    void atMostOnceTest(@InjectClusterClient ClusterClient<?> clusterClient) throws Exception {
        StreamExecutionEnvironment env = getEnv(CheckpointingMode.EXACTLY_ONCE);

        List<String> messages = getRandomMessages(100);
        messageLatch = new CountDownLatch(messages.size());

        DataStream<String> stream = addSourceOn(env, ConsistencyMode.AT_MOST_ONCE);

        CollectSink.addOnStream(stream);
        JobGraph jobGraph = env.getStreamGraph().getJobGraph();
        CompletableFuture<JobID> jobIdFuture = clusterClient.submitJob(jobGraph);

        sendToRabbit(messages);
        messageLatch.await(10, TimeUnit.MINUTES);
        assertThat(messages).containsAll(sinkedMessages);
        clusterClient.cancel(jobIdFuture.get()).get();
        CommonTestUtils.waitUntilCondition(
                () -> clusterClient.getJobStatus(jobGraph.getJobID()).get() == JobStatus.CANCELED,
                1000);
    }

    // --------------- at least once ---------------
    @Test
    void atLeastOnceTest(@InjectClusterClient MiniClusterClient miniClusterClient)
            throws Exception {
        StreamExecutionEnvironment env = getEnv(CheckpointingMode.AT_LEAST_ONCE);

        List<String> messages = getRandomMessages(100);
        messageLatch = new CountDownLatch(messages.size());

        DataStream<String> stream = addSourceOn(env, ConsistencyMode.AT_LEAST_ONCE);
        CollectSink.addOnStream(stream);
        CompletableFuture<JobID> jobIdFuture =
                miniClusterClient.submitJob(env.getStreamGraph().getJobGraph());

        sendToRabbit(messages);
        messageLatch.await();
        assertThat(sinkedMessages).containsAll(messages);
        miniClusterClient.cancel(jobIdFuture.get()).get();
    }

    @Test
    void atLeastOnceFailureTest(@InjectClusterClient ClusterClient<?> clusterClient)
            throws Exception {
        // An exception is thrown in the MapFunction in order to trigger a restart of Flink and it
        // is assured that the source receives the messages again.
        StreamExecutionEnvironment env = getEnv(CheckpointingMode.AT_LEAST_ONCE);

        DataStream<String> stream = addSourceOn(env, ConsistencyMode.AT_LEAST_ONCE);
        List<String> messages = getSequentialMessages(100);
        failAtNthMessage = 30;
        messageLatch = new CountDownLatch(messages.size() + failAtNthMessage - 1);
        CollectSink.addOnStream(stream);
        sendToRabbit(messages);

        JobGraph jobGraph = env.getStreamGraph().getJobGraph();

        CompletableFuture<JobID> jobIdFuture = clusterClient.submitJob(jobGraph);

        messageLatch.await();

        assertThat(sinkedMessages).containsAll(messages);
        clusterClient.cancel(jobIdFuture.get()).get();

        CommonTestUtils.waitUntilCondition(
                () -> clusterClient.getJobStatus(jobGraph.getJobID()).get() == JobStatus.CANCELED,
                1000);
    }

    // --------------- exactly once ---------------
    @Test
    void exactlyOnceTest(@InjectClusterClient MiniClusterClient miniClusterClient)
            throws Exception {
        StreamExecutionEnvironment env = getEnv(CheckpointingMode.EXACTLY_ONCE);

        List<String> messages = getRandomMessages(1000);
        messageLatch = new CountDownLatch(messages.size());

        DataStream<String> stream = addSourceOn(env, ConsistencyMode.EXACTLY_ONCE);
        CollectSink.addOnStream(stream);

        CompletableFuture<JobID> jobIdFuture =
                miniClusterClient.submitJob(env.getStreamGraph().getJobGraph());

        // use messages as correlation ids here
        sendToRabbit(messages, messages);
        messageLatch.await();

        assertThat(messages).contains(sinkedMessages.toArray(new String[0]));
        miniClusterClient.cancel(jobIdFuture.get()).get();
    }

    @Test
    void exactlyOnceFilterCorrelationIdsTest(
            @InjectClusterClient MiniClusterClient miniClusterClient) throws Exception {
        StreamExecutionEnvironment env = getEnv(CheckpointingMode.EXACTLY_ONCE);

        List<String> messages = getRandomMessages(5);
        List<String> correlationIds = Arrays.asList("1", "2", "3", "3", "3");
        messageLatch = new CountDownLatch(3);

        DataStream<String> stream = addSourceOn(env, ConsistencyMode.EXACTLY_ONCE);
        CollectSink.addOnStream(stream);
        CompletableFuture<JobID> jobIdFuture =
                miniClusterClient.submitJob(env.getStreamGraph().getJobGraph());

        sendToRabbit(messages, correlationIds);

        messageLatch.await();

        List<String> expectedMessages = messages.subList(0, 3);
        assertThat(sinkedMessages).isEqualTo(expectedMessages);
        miniClusterClient.cancel(jobIdFuture.get()).get();
    }

    /**
     * This test is supposed to check if we receive all messages once again which were polled after
     * the checkpoint and before the exception thrown by the test. Thus, these messages were not yet
     * acknowledged to RabbitMQ and therefore will be consumed once again after the recovery. This
     * checks that messages will not be lost on failures.
     *
     * <p>The CollectSink has no checkpoint logic and will collect message twice. The test expect
     * that all messages before the checkpoint are received twice by the CollectSink.
     *
     * @throws Exception something not supposed failed
     */
    @Test
    void exactlyOnceWithFailureAndMessageDuplicationTest(
            @InjectClusterClient MiniClusterClient miniClusterClient) throws Exception {
        // An exception is thrown in order to trigger a restart of Flink and it
        // is assured that the system receives the messages only once. We disable
        // (by setting the interval higher than the test duration) checkpoint to
        // expect receiving all pre-exception messages once again.
        StreamExecutionEnvironment env = getEnv(CheckpointingMode.EXACTLY_ONCE);

        //        env.enableCheckpointing(500000);
        DataStream<String> stream = addSourceOn(env, ConsistencyMode.EXACTLY_ONCE);

        List<String> messages = getRandomMessages(100);

        int originalFailAthNthMessage = 30;
        failAtNthMessage = originalFailAthNthMessage;
        messageLatch = new CountDownLatch(messages.size() + failAtNthMessage - 1);
        CollectSink.addOnStream(stream);
        CompletableFuture<JobID> jobIdFuture =
                miniClusterClient.submitJob(env.getStreamGraph().getJobGraph());

        sendToRabbit(messages, messages);
        messageLatch.await();

        List<String> expectedMessage =
                sinkedMessages.subList(originalFailAthNthMessage - 1, sinkedMessages.size());
        assertThat(messages).isEqualTo(expectedMessage);

        miniClusterClient.cancel(jobIdFuture.get()).get();
    }

    /**
     * This test checks that messages which were consumed and polled before a successful and
     * completed checkpoint will not be consumed from RabbitMQ a second time if a failure happens.
     * This mean that these messages will not be polled a second time from Flink (after recovery) as
     * well and therefore no duplicated are expected in the CollectSink.
     *
     * @throws Exception something not supposed failed
     */
    @Test
    void exactlyOnceWithFailureWithNoMessageDuplicationTest(
            @InjectClusterClient MiniClusterClient miniClusterClient) throws Exception {
        StreamExecutionEnvironment env = getEnv(CheckpointingMode.EXACTLY_ONCE);
        // env.enableCheckpointing(1000);
        DataStream<String> stream = addSourceOn(env, ConsistencyMode.EXACTLY_ONCE);

        List<String> messages = getSequentialMessages(60);
        List<String> messagesA = messages.subList(0, 30);
        List<String> messagesB = messages.subList(30, messages.size());

        failAtNthMessage = messagesA.size() + 1;
        messageLatch = new CountDownLatch(messagesA.size() + messagesB.size());

        CollectSink.addOnStream(stream);
        CompletableFuture<JobID> jobIdFuture =
                miniClusterClient.submitJob(env.getStreamGraph().getJobGraph());

        // Send first batch of messages
        sendToRabbit(messagesA, messagesA);

        // Wait for successful checkpoints to ensure the previous message are acknowledged and
        // thus will not be polled a second .
        checkpointLatch = new CountDownLatch(2);
        checkpointLatch.await();

        // Send second batch of messages
        sendToRabbit(messagesB, messagesB);

        messageLatch.await();

        // Expect all message to be received without duplications
        assertThat(messages).isEqualTo(sinkedMessages);

        miniClusterClient.cancel(jobIdFuture.get()).get();
    }
}
