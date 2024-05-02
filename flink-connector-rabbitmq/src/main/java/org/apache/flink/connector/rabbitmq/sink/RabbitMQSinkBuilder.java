package org.apache.flink.connector.rabbitmq.sink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.rabbitmq.common.DefaultRabbitMQMessageConverter;
import org.apache.flink.connector.rabbitmq.common.RabbitMQConnectionConfig;
import org.apache.flink.connector.rabbitmq.common.RabbitMQMessageConverter;
import org.apache.flink.connector.rabbitmq.common.SerializableReturnListener;

import java.util.Optional;

import static org.apache.flink.connector.rabbitmq.common.Constants.DEFAULT_FAIL_ON_ERROR;
import static org.apache.flink.connector.rabbitmq.common.Constants.DEFAULT_MAX_INFLIGHT;

/**
 * A builder for creating a {@link RabbitMQSink}.
 *
 * <p>The builder uses the following parameters to build a {@link RabbitMQSink}:
 *
 * <ul>
 *   <li>{@link RabbitMQConnectionConfig} for the connection to RabbitMQ.
 *   <li>{@link SerializationSchema} for serializing the input data.
 *   <li>{@link RabbitMQMessageConverter} for converting the input data to {@link
 *       org.apache.flink.connector.rabbitmq.common.RabbitMQMessage}.
 *   <li>{@link SerializableReturnListener} for handling returned messages.
 *   <li>{@code queueName} for the name of the queue to send messages to.
 *   <li>{@code maximumInflightMessages} for the maximum number of in-flight messages.
 *   <li>{@code failOnError} for whether to fail on an error.
 * </ul>
 *
 * <p>It can be used as follows:
 *
 * <pre>{@code
 * RabbitMQSink<String> rabbitMQSink = {@code RabbitMQSinkBuilder}.<String>builder()
 *     .setConnectionConfig(connectionConfig)
 *     .setSerializationSchema(new SimpleStringSchema())
 *     .setMessageConverter(new SimpleStringMessageConverter())
 *     .setReturnListener(new SimpleReturnListener())
 *     .setQueueName("queue")
 *     .setMaximumInflightMessages(10)
 *     .setFailOnError(true)
 *     .build();
 *
 * }</pre>
 *
 * @param <T>
 */
@PublicEvolving
public class RabbitMQSinkBuilder<T> {

    private RabbitMQConnectionConfig connectionConfig;

    private SerializationSchema<T> serializationSchema;

    private RabbitMQMessageConverter<T> messageConverter;

    private SerializableReturnListener returnListener;

    private String queueName;

    private Integer maximumInflightMessages;

    private Boolean failOnError;

    public RabbitMQSinkBuilder<T> setConnectionConfig(RabbitMQConnectionConfig connectionConfig) {
        this.connectionConfig = connectionConfig;
        return this;
    }

    public RabbitMQSinkBuilder<T> setSerializationSchema(
            SerializationSchema<T> serializationSchema) {
        this.serializationSchema = serializationSchema;
        return this;
    }

    public RabbitMQSinkBuilder<T> setMessageConverter(
            RabbitMQMessageConverter<T> messageConverter) {
        this.messageConverter = messageConverter;
        return this;
    }

    public RabbitMQSinkBuilder<T> setReturnListener(SerializableReturnListener returnListener) {
        this.returnListener = returnListener;
        return this;
    }

    public RabbitMQSinkBuilder<T> setQueueName(String queueName) {
        this.queueName = queueName;
        return this;
    }

    public RabbitMQSinkBuilder<T> setMaximumInflightMessages(int maximumInflightMessages) {
        this.maximumInflightMessages = maximumInflightMessages;
        return this;
    }

    public RabbitMQSinkBuilder<T> setFailOnError(boolean failOnError) {
        this.failOnError = failOnError;
        return this;
    }

    public RabbitMQSink<T> build() {
        return new RabbitMQSink<>(
                connectionConfig,
                serializationSchema,
                Optional.ofNullable(messageConverter)
                        .orElse(new DefaultRabbitMQMessageConverter<>()),
                returnListener,
                queueName,
                Optional.ofNullable(maximumInflightMessages).orElse(DEFAULT_MAX_INFLIGHT),
                Optional.ofNullable(failOnError).orElse(DEFAULT_FAIL_ON_ERROR));
    }
}
