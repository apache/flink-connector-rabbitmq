package org.apache.flink.connector.rabbitmq.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.rabbitmq.common.Constants;
import org.apache.flink.connector.rabbitmq.common.DefaultRabbitMQMessageConverter;
import org.apache.flink.connector.rabbitmq.common.RabbitMQConnectionConfig;
import org.apache.flink.connector.rabbitmq.common.RabbitMQMessageConverter;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests for the {@link RabbitMQSink}. */
public class RabbitMQSinkBuilderTest {

    @Test
    void buildSinkThrowsExceptionOnNullRabbitMQConnectionConfig() {
        RabbitMQMessageConverter<String> messageConverter = new DefaultRabbitMQMessageConverter<>();
        String queueName = "test-queue";

        RabbitMQSinkBuilder<String> sinkBuilder = new RabbitMQSinkBuilder<>();
        sinkBuilder
                .setSerializationSchema(new SimpleStringSchema())
                .setMessageConverter(messageConverter)
                .setQueueName(queueName);

        Assertions.assertThatExceptionOfType(NullPointerException.class)
                .isThrownBy(sinkBuilder::build)
                .withMessageContaining("connectionConfig cannot be null");
    }

    @Test
    void buildSinkThrowsExceptionOnNullQueueName() {
        RabbitMQMessageConverter<String> messageConverter = new DefaultRabbitMQMessageConverter<>();

        RabbitMQSinkBuilder<String> sinkBuilder = new RabbitMQSinkBuilder<>();
        sinkBuilder
                .setSerializationSchema(new SimpleStringSchema())
                .setMessageConverter(messageConverter)
                .setConnectionConfig(getRabbitMQConnectionConfig());

        Assertions.assertThatExceptionOfType(NullPointerException.class)
                .isThrownBy(sinkBuilder::build)
                .withMessageContaining("queueName cannot be null");
    }

    @Test
    void buildSinkThrowsExceptionOnNullSerializationSchema() {
        RabbitMQMessageConverter<String> messageConverter = new DefaultRabbitMQMessageConverter<>();

        RabbitMQSinkBuilder<String> sinkBuilder = new RabbitMQSinkBuilder<>();
        sinkBuilder
                .setQueueName("test-queue")
                .setMessageConverter(messageConverter)
                .setConnectionConfig(getRabbitMQConnectionConfig());

        Assertions.assertThatExceptionOfType(NullPointerException.class)
                .isThrownBy(sinkBuilder::build)
                .withMessageContaining("serializationSchema cannot be null");
    }

    @Test
    void buildSinkUsesDefaultValues() {
        RabbitMQMessageConverter<String> messageConverter = new DefaultRabbitMQMessageConverter<>();
        String queueName = "test-queue";
        RabbitMQSinkBuilder<String> sinkBuilder = new RabbitMQSinkBuilder<>();

        RabbitMQSink<String> sink =
                sinkBuilder
                        .setMessageConverter(messageConverter)
                        .setQueueName(queueName)
                        .setSerializationSchema(new SimpleStringSchema())
                        .setConnectionConfig(getRabbitMQConnectionConfig())
                        .build();

        Assertions.assertThat(sink)
                .hasFieldOrPropertyWithValue(
                        "maximumInflightMessages", Constants.DEFAULT_MAX_INFLIGHT);

        Assertions.assertThat(sink)
                .hasFieldOrPropertyWithValue("failOnError", Constants.DEFAULT_FAIL_ON_ERROR);
    }

    private RabbitMQConnectionConfig getRabbitMQConnectionConfig() {
        return new RabbitMQConnectionConfig.Builder<>()
                .setHost("test-host")
                .setPort(5672)
                .setVirtualHost("/")
                .setUserName("guest")
                .setPassword("guest")
                .build();
    }
}
