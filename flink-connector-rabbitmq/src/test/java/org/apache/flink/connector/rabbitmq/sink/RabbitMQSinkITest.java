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

package org.apache.flink.connector.rabbitmq.sink;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.rabbitmq.common.ConsistencyMode;
import org.apache.flink.connector.rabbitmq.common.RabbitMQConnectionConfig;
import org.apache.flink.connector.rabbitmq.common.RabbitMQContainerClient;
import org.apache.flink.core.testutils.AllCallbackWrapper;
import org.apache.flink.core.testutils.TestContainerExtension;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
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
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The tests for the RabbitMQ sink with different consistency modes. As the tests are working a lot
 * with timeouts to uphold stream it is possible that tests might fail.
 */
public class RabbitMQSinkITest {

    private static AtomicBoolean shouldFail;

    public static final Logger LOG = LoggerFactory.getLogger(RabbitMQSinkITest.class);

    private static final int RABBITMQ_PORT = 5672;

    private RabbitMQContainer rabbitMq;

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

    private RabbitMQContainerClient<String> client;
    protected StreamExecutionEnvironment env;

    public static Configuration conf = new Configuration();

    @BeforeEach
    public void setUpContainerClient() {
        shouldFail = new AtomicBoolean(true);
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, 1000));
        rabbitMq = rabbitMqCallbackWrapper.getCustomExtension().getTestContainer();
        this.client =
                new RabbitMQContainerClient<>(
                        rabbitMqCallbackWrapper.getCustomExtension().getTestContainer());
    }

    public RabbitMQContainerClient<String> addSinkOn(
            DataStream<String> stream, ConsistencyMode consistencyMode, int countDownLatchSize)
            throws IOException, TimeoutException {
        RabbitMQContainerClient<String> client =
                new RabbitMQContainerClient<>(
                        rabbitMq, new SimpleStringSchema(), countDownLatchSize);
        String queueName = client.createQueue();
        final RabbitMQConnectionConfig connectionConfig =
                new RabbitMQConnectionConfig.Builder()
                        .setHost(rabbitMq.getHost())
                        .setVirtualHost("/")
                        .setUserName(rabbitMq.getAdminUsername())
                        .setPassword(rabbitMq.getAdminPassword())
                        .setPort(rabbitMq.getMappedPort(RABBITMQ_PORT))
                        .build();

        RabbitMQSink<String> sink =
                RabbitMQSink.<String>builder()
                        .setConnectionConfig(connectionConfig)
                        .setQueueName(queueName)
                        .setSerializationSchema(new SimpleStringSchema())
                        .setConsistencyMode(consistencyMode)
                        .build();
        stream.sinkTo(sink).setParallelism(1);
        return client;
    }

    protected List<String> getRandomMessages(int numberOfMessages) {
        List<String> messages = new ArrayList<>();
        for (int i = 0; i < numberOfMessages; i++) {
            messages.add(UUID.randomUUID().toString());
        }
        return messages;
    }

    private static class GeneratorFailureSource implements SourceFunction<String> {

        private final BlockingQueue<String> messagesToSend;
        private int failAtNthMessage;

        public GeneratorFailureSource(BlockingQueue<String> messagesToSend, int failAtNthMessage) {
            this.messagesToSend = messagesToSend;
            this.failAtNthMessage = failAtNthMessage;
            shouldFail.set(true);
        }

        @Override
        public void run(SourceContext<String> sourceContext) throws Exception {
            while (true) {
                if (failAtNthMessage == 0 && shouldFail.get()) {
                    shouldFail.set(false);
                    throw new Exception("Supposed to Fail");
                }
                failAtNthMessage -= 1;
                String message = messagesToSend.take();
                sourceContext.collect(message);
            }
        }

        @Override
        public void cancel() {}
    }

    @Test
    public void atMostOnceTest(@InjectClusterClient ClusterClient<?> clusterClient)
            throws Exception {
        List<String> messages = getRandomMessages(100);

        DataStream<String> stream = env.fromCollection(messages);
        RabbitMQContainerClient<String> client =
                addSinkOn(stream, ConsistencyMode.AT_MOST_ONCE, messages.size());
        CompletableFuture<JobID> jobIDCompletableFuture =
                clusterClient.submitJob(env.getStreamGraph().getJobGraph());
        client.await();
        clusterClient.cancel(jobIDCompletableFuture.get()).get();
        List<String> receivedMessages = client.getConsumedMessages();
        assertThat(messages).isEqualTo(receivedMessages);
    }

    @Test
    public void atLeastOnceTest(@InjectClusterClient ClusterClient<?> clusterClient)
            throws Exception {
        List<String> messages = getRandomMessages(100);
        DataStream<String> stream = env.fromCollection(messages);
        RabbitMQContainerClient<String> client =
                addSinkOn(stream, ConsistencyMode.AT_LEAST_ONCE, messages.size());
        CompletableFuture<JobID> jobIDCompletableFuture =
                clusterClient.submitJob(env.getStreamGraph().getJobGraph());
        client.await();
        clusterClient.cancel(jobIDCompletableFuture.get()).get();

        List<String> receivedMessages = client.getConsumedMessages();
        assertThat(messages).isEqualTo(receivedMessages);
    }

    @Test
    public void atLeastOnceWithFlinkFailureTest(@InjectClusterClient ClusterClient<?> clusterClient)
            throws Exception {
        LinkedBlockingQueue<String> messages = new LinkedBlockingQueue<>(getRandomMessages(100));

        GeneratorFailureSource source = new GeneratorFailureSource(messages, 30);

        DataStream<String> stream = env.addSource(source);
        RabbitMQContainerClient<String> client =
                addSinkOn(stream, ConsistencyMode.AT_LEAST_ONCE, messages.size() + 30);

        CompletableFuture<JobID> jobIDCompletableFuture =
                clusterClient.submitJob(env.getStreamGraph().getJobGraph());
        client.await();
        clusterClient.cancel(jobIDCompletableFuture.get()).get();

        List<String> receivedMessages = client.getConsumedMessages();
        assertThat(receivedMessages).containsAll(messages);
    }

    @Test
    public void exactlyOnceTest(@InjectClusterClient ClusterClient<?> clusterClient)
            throws Exception {
        LinkedBlockingQueue<String> messages = new LinkedBlockingQueue<>(getRandomMessages(100));
        env.enableCheckpointing(100);

        GeneratorFailureSource source = new GeneratorFailureSource(messages, -1);
        DataStream<String> stream = env.addSource(source);
        RabbitMQContainerClient<String> client =
                addSinkOn(stream, ConsistencyMode.EXACTLY_ONCE, messages.size());

        CompletableFuture<JobID> jobIDCompletableFuture =
                clusterClient.submitJob(env.getStreamGraph().getJobGraph());
        client.await();
        clusterClient.cancel(jobIDCompletableFuture.get()).get();

        assertThat(messages.toArray()).isEqualTo(client.getConsumedMessages().toArray());
    }

    @Test
    public void exactlyOnceWithFlinkFailureTest(@InjectClusterClient ClusterClient<?> clusterClient)
            throws Exception {
        LinkedBlockingQueue<String> messages = new LinkedBlockingQueue<>(getRandomMessages(100));
        env.enableCheckpointing(100);

        GeneratorFailureSource source = new GeneratorFailureSource(messages, 80);
        DataStream<String> stream = env.addSource(source);
        RabbitMQContainerClient<String> client =
                addSinkOn(stream, ConsistencyMode.EXACTLY_ONCE, messages.size());

        CompletableFuture<JobID> jobIDCompletableFuture =
                clusterClient.submitJob(env.getStreamGraph().getJobGraph());
        client.await();
        clusterClient.cancel(jobIDCompletableFuture.get()).get();

        assertThat(messages.toArray()).isEqualTo(client.getConsumedMessages().toArray());
    }
}
