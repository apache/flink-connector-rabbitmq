package org.apache.flink.connector.rabbitmq.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.connector.rabbitmq.common.RabbitMQConnectionConfig;
import org.apache.flink.connector.rabbitmq.common.RabbitMQMessage;
import org.apache.flink.connector.rabbitmq.common.RabbitMQMessageConverter;
import org.apache.flink.connector.rabbitmq.common.SerializableReturnListener;
import org.apache.flink.connector.rabbitmq.common.util.RabbitMQConnectionUtil;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmCallback;
import com.rabbitmq.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.TreeMap;
import java.util.concurrent.TimeoutException;

/**
 * A {@link SinkWriter} to produce data into RabbitMQ. The sink uses the {@link
 * RabbitMQConnectionConfig} to create a connection to RabbitMQ, uses {@link
 * RabbitMQMessageConverter} to convert the input data to {@link
 * org.apache.flink.connector.rabbitmq.common.RabbitMQMessage}, and uses {@link
 * com.rabbitmq.client.Channel} to send messages to the specified queue.
 *
 * <p>The sink writer is stateless and blocks for new writes if the number of inflight messages
 * exceeds the maximum number of inflight messages' parameter. The sink writer also blocks for
 * inflight messages before taking snapshots.
 *
 * @param <T> input type for the sink.
 */
@Internal
public class RabbitMQSinkWriter<T> implements SinkWriter<T> {
    private static final Logger LOG = LoggerFactory.getLogger(RabbitMQSinkWriter.class);

    /** The name of the queue to send messages to. */
    private final String queueName;

    /**
     * The message converter to convert the input data to {@link RabbitMQMessage}.
     *
     * <p>The MessageConverter also is responsible of defining the routing schema of message
     * publishing, by implementing the {@link RabbitMQMessageConverter#supportsExchangeRouting()}
     * method to signal if the converter supports exchange routing.
     */
    private final RabbitMQMessageConverter<T> messageConverter;

    private final SerializationSchema<T> serializationSchema;

    private final SerializableReturnListener returnListener;

    /* Counter for number of bytes this sink has attempted to send to the destination. */
    private final Counter numBytesOutCounter;

    /* Counter for number of records this sink has attempted to send to the destination. */
    private final Counter numRecordsOutCounter;

    private final MailboxExecutor mailboxExecutor;

    /**
     * The maximum number of inflight messages. The sink writer blocks for new writes if the number
     * of inflight messages exceeds this value.
     */
    private final int maximumInflightMessages;

    /* Flag to indicate if the sink should fail on error. */
    private final boolean failOnError;

    /**
     * Map to hold inflightMessages, the {@code getSuccessConfirmCallback} and {@code
     * getFailureConfirmCallback} are triggered using sequence numbers hence we keep the mapping of
     * sequence number to the message. We are using a sorted map to evict all inflight requests with
     * sequence number less than or equal to the sequence number of the message that was
     * acknowledged if flagged to acknowledge all previous messages.
     */
    private final TreeMap<Long, RabbitMQMessage<T>> inflightMessages;

    private Connection connection;
    private Channel channel;

    public RabbitMQSinkWriter(
            WriterInitContext context,
            RabbitMQConnectionConfig connectionConfig,
            String queueName,
            RabbitMQMessageConverter<T> messageConverter,
            SerializationSchema<T> serializationSchema,
            SerializableReturnListener returnListener,
            int maximumInflightMessages,
            boolean failOnError)
            throws IOException {
        this(
                context,
                queueName,
                messageConverter,
                serializationSchema,
                returnListener,
                maximumInflightMessages,
                failOnError);
        Preconditions.checkNotNull(connectionConfig, "connectionConfig cannot be null");
        try {
            Connection connection =
                    RabbitMQConnectionUtil.getConnectionFactory(connectionConfig).newConnection();
            initializeConnection(connection);
        } catch (TimeoutException e) {
            throw new IOException("Failed to create connection", e);
        }
    }

    @VisibleForTesting
    RabbitMQSinkWriter(
            WriterInitContext context,
            String queueName,
            RabbitMQMessageConverter<T> messageConverter,
            SerializationSchema<T> serializationSchema,
            SerializableReturnListener returnListener,
            int maximumInflightMessages,
            boolean failOnError) {
        Preconditions.checkNotNull(context, "context cannot be null");

        this.mailboxExecutor = context.getMailboxExecutor();
        this.maximumInflightMessages = maximumInflightMessages;
        this.failOnError = failOnError;

        SinkWriterMetricGroup metricGroup = context.metricGroup();
        this.queueName = queueName;
        this.messageConverter = messageConverter;
        this.serializationSchema = serializationSchema;
        this.returnListener = returnListener;

        this.numBytesOutCounter = metricGroup.getIOMetricGroup().getNumBytesOutCounter();
        this.numRecordsOutCounter = metricGroup.getIOMetricGroup().getNumRecordsOutCounter();
        this.inflightMessages = new TreeMap<>();
    }

    @VisibleForTesting
    void initializeConnection(Connection connection) throws IOException {
        this.connection = connection;
        this.channel = connection.createChannel();
        channel.addReturnListener(returnListener);
        channel.addConfirmListener(getSuccessConfirmCallback(), getFailureConfirmCallback());
        channel.confirmSelect();
        channel.queueDeclare(queueName, true, false, false, null);
    }

    @Override
    public void write(T t, Context context) throws IOException, InterruptedException {
        awaitInflightMessagesBelow(maximumInflightMessages);
        RabbitMQMessage<T> recordMessage = messageConverter.toRabbitMQMessage(t);
        publishMessage(recordMessage);
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        awaitInflightMessagesBelow(1);
    }

    @Override
    public void close() throws Exception {
        awaitInflightMessagesBelow(1);
        channel.close();
        connection.close();
    }

    private void awaitInflightMessagesBelow(int maximumInflightMessages)
            throws InterruptedException {
        while (inflightMessages.size() >= maximumInflightMessages) {
            mailboxExecutor.yield();
        }
    }

    private void publishMessage(RabbitMQMessage<T> recordMessage) throws IOException {
        byte[] message = serializationSchema.serialize(recordMessage.getMessage());
        recordMessage.setSerializedSize((long) message.length);
        Long sequenceNumber = channel.getNextPublishSeqNo();

        if (messageConverter.supportsExchangeRouting()) {
            Preconditions.checkArgument(
                    returnListener != null
                            || !(recordMessage.isImmediate() || recordMessage.isMandatory()),
                    "Return listener must be set if immediate or mandatory delivery is requested");

            channel.basicPublish(
                    recordMessage.getExchange(),
                    recordMessage.getRoutingKey(),
                    recordMessage.isMandatory(),
                    recordMessage.isImmediate(),
                    recordMessage.getMessageProperties(),
                    message);

        } else {
            channel.basicPublish(
                    recordMessage.getExchange(),
                    queueName,
                    recordMessage.getMessageProperties(),
                    message);
        }

        inflightMessages.put(sequenceNumber, recordMessage);
    }

    private ConfirmCallback getSuccessConfirmCallback() {
        return (seqNo, acknowledgePrevious) ->
                mailboxExecutor.execute(
                        () -> {
                            if (acknowledgePrevious) {
                                LOG.debug(
                                        "Acknowledge all messages with sequence number less than or equal to {}",
                                        seqNo);
                                while (!inflightMessages.isEmpty()
                                        && inflightMessages.firstKey() <= (Long) seqNo) {
                                    RabbitMQMessage<T> message =
                                            inflightMessages.remove(inflightMessages.firstKey());
                                    numBytesOutCounter.inc(message.getSerializedSize());
                                    numRecordsOutCounter.inc();
                                }
                            } else {
                                LOG.debug("Acknowledge message with sequence number {}", seqNo);
                                RabbitMQMessage<T> message = inflightMessages.remove(seqNo);
                                numBytesOutCounter.inc(message.getSerializedSize());
                                numRecordsOutCounter.inc();
                            }
                        },
                        "Acknowledge message with sequence number " + seqNo);
    }

    private ConfirmCallback getFailureConfirmCallback() {
        return (seqNo, acknowledgePrevious) ->
                mailboxExecutor.execute(
                        () -> {
                            if (failOnError) {
                                LOG.error(
                                        "Failed to send message with sequence number {} and payload {}",
                                        seqNo,
                                        inflightMessages.get(seqNo).getMessage());
                                throw new FlinkRuntimeException(
                                        String.format(
                                                "Failed to send message with sequence number %d and payload %s",
                                                seqNo, inflightMessages.get(seqNo).getMessage()));
                            }
                            LOG.warn(
                                    "Resending failed message with sequence number {} and payload {}",
                                    seqNo,
                                    inflightMessages.get(seqNo).getMessage());
                            publishMessage(inflightMessages.remove(seqNo));
                        },
                        "Handle failure for message with sequence number " + seqNo);
    }
}
