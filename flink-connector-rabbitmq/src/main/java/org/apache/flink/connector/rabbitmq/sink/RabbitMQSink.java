package org.apache.flink.connector.rabbitmq.sink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.connector.rabbitmq.common.RabbitMQConnectionConfig;
import org.apache.flink.connector.rabbitmq.common.RabbitMQMessageConverter;
import org.apache.flink.connector.rabbitmq.common.SerializableReturnListener;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

/**
 * A RabbitMQ {@link Sink} to produce data into RabbitMQ. The sink uses the {@link
 * RabbitMQConnectionConfig} to create a connection to RabbitMQ, uses {@link
 * RabbitMQMessageConverter} to convert the input data to {@link
 * org.apache.flink.connector.rabbitmq.common.RabbitMQMessage}, and uses {@link
 * com.rabbitmq.client.Channel} to send messages to the specified queue.
 *
 * @param <T> input type for the sink.
 */
@PublicEvolving
public class RabbitMQSink<T> implements Sink<T> {

    private final RabbitMQConnectionConfig connectionConfig;

    private final SerializationSchema<T> serializationSchema;

    private final RabbitMQMessageConverter<T> messageConverter;

    private final SerializableReturnListener returnListener;

    private final String queueName;

    private final int maximumInflightMessages;

    private final boolean failOnError;

    public RabbitMQSink(
            RabbitMQConnectionConfig connectionConfig,
            SerializationSchema<T> serializationSchema,
            RabbitMQMessageConverter<T> messageConverter,
            SerializableReturnListener returnListener,
            String queueName,
            int maximumInflightMessages,
            boolean failOnError) {
        Preconditions.checkNotNull(queueName, "queueName cannot be null");
        Preconditions.checkNotNull(messageConverter, "messageConverter cannot be null");
        Preconditions.checkNotNull(serializationSchema, "serializationSchema cannot be null");
        Preconditions.checkNotNull(connectionConfig, "connectionConfig cannot be null");
        Preconditions.checkArgument(
                maximumInflightMessages > 0, "maximumInflightMessages must be greater than 0");

        this.connectionConfig = connectionConfig;
        this.serializationSchema = serializationSchema;
        this.messageConverter = messageConverter;
        this.returnListener = returnListener;
        this.queueName = queueName;
        this.maximumInflightMessages = maximumInflightMessages;
        this.failOnError = failOnError;
    }

    @Override
    public SinkWriter<T> createWriter(InitContext initContext) throws IOException {
        throw new UnsupportedOperationException(
                "Creating writer with Deprecated InitContext is not supported. Please use WriterInitContext.");
    }

    @Override
    public SinkWriter<T> createWriter(WriterInitContext context) throws IOException {
        return new RabbitMQSinkWriter<>(
                context,
                connectionConfig,
                queueName,
                messageConverter,
                serializationSchema,
                returnListener,
                maximumInflightMessages,
                failOnError);
    }

    public static <T> RabbitMQSinkBuilder<T> builder() {
        return new RabbitMQSinkBuilder<T>();
    }
}
