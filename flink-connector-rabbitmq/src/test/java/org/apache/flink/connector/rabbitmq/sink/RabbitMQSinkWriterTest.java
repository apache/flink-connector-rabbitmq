package org.apache.flink.connector.rabbitmq.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.connector.base.sink.writer.TestSinkInitContext;
import org.apache.flink.connector.base.sink.writer.TestSinkInitContextAnyThreadMailbox;
import org.apache.flink.connector.rabbitmq.common.DefaultRabbitMQMessageConverter;
import org.apache.flink.connector.rabbitmq.common.RabbitMQConnectionConfig;
import org.apache.flink.connector.rabbitmq.common.SerializableReturnListener;
import org.apache.flink.connector.rabbitmq.sink.util.TestChannel;
import org.apache.flink.connector.rabbitmq.sink.util.TestConnection;
import org.apache.flink.connector.rabbitmq.sink.util.TestMessageConverter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.util.FlinkRuntimeException;

import com.rabbitmq.client.AMQP;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

/** Test class for the {@link RabbitMQSinkWriter}. */
public class RabbitMQSinkWriterTest {
    private static final String QUEUE_NAME = "test-queue";

    private static final Integer MAXIMUM_INFLIGHT_MESSAGES = 3;

    @Test
    void testWriterInitializationDeclaresQueue() throws IOException {
        TestConnection connection = getRabbitMQTestConnection();
        TestChannel channel = getRabbitMQTestChannel();
        connection.setChannelSupplier(() -> channel);

        RabbitMQSinkWriter<String> writer = getDefaultWriter(null);
        writer.initializeConnection(connection);

        Assertions.assertThat(channel.isQueueDeclared()).isTrue();
    }

    @Test
    void writerInitializationDeclaresExchange() throws IOException {
        TestConnection connection = getRabbitMQTestConnection();
        TestChannel channel = getRabbitMQTestChannel();
        connection.setChannelSupplier(() -> channel);

        RabbitMQSinkWriter<String> writer = getDefaultWriter(null);
        writer.initializeConnection(connection);

        Assertions.assertThat(channel.isOpen()).isTrue();
        Assertions.assertThat(connection.isOpen()).isTrue();
    }

    @Test
    void writerInitializationAddsReturnListener() throws IOException {
        TestConnection connection = getRabbitMQTestConnection();
        TestChannel channel = getRabbitMQTestChannel();
        connection.setChannelSupplier(() -> channel);

        SerializableReturnListener returnListener = (i, s, s1, s2, basicProperties, bytes) -> {};

        RabbitMQSinkWriter<String> writer = getDefaultWriter(returnListener);
        writer.initializeConnection(connection);

        Assertions.assertThat(channel.getReturnListener()).isSameAs(returnListener);
    }

    @Test
    void writerInitializationAddsConfirmListeners() throws IOException {
        TestConnection connection = getRabbitMQTestConnection();
        TestChannel channel = getRabbitMQTestChannel();
        connection.setChannelSupplier(() -> channel);

        RabbitMQSinkWriter<String> writer = getDefaultWriter(null);
        writer.initializeConnection(connection);

        Assertions.assertThat(channel.getOnSuccess()).isNotNull();
        Assertions.assertThat(channel.getOnFail()).isNotNull();
    }

    @Test
    void writeMessageDeliversMessageWithChannel() throws IOException, InterruptedException {
        TestConnection connection = getRabbitMQTestConnection();
        TestChannel channel = getRabbitMQTestChannel();
        connection.setChannelSupplier(() -> channel);

        RabbitMQSinkWriter<String> writer = getDefaultWriter(null);
        writer.initializeConnection(connection);
        String message = "test-message";
        writeMessageToWriter(writer, message, channel);
        writer.flush(false);

        Assertions.assertThat(channel.getDeliveredMessages()).containsExactly(message);
    }

    @Test
    void writeMessageDeliversMessageCallsReturnListener() throws IOException, InterruptedException {
        TestConnection connection = getRabbitMQTestConnection();
        TestChannel channel = getRabbitMQTestChannel();
        connection.setChannelSupplier(() -> channel);

        AtomicReference<String> returnedMessage = new AtomicReference<>("");
        SerializableReturnListener returnListener =
                (i, s, s1, s2, basicProperties, bytes) -> {
                    returnedMessage.set("returned " + new String(bytes));
                };

        RabbitMQSinkWriter<String> writer = getDefaultWriter(returnListener);
        writer.initializeConnection(connection);
        String message = "test-message";
        writeMessageToWriter(writer, message, channel);
        writer.flush(false);

        Assertions.assertThat(channel.getDeliveredMessages()).containsExactly(message);
        Assertions.assertThat(returnedMessage.get()).isEqualTo("returned test-message");
    }

    @Test
    void writeMessageIncrementsMetricsOnDelivery() throws IOException, InterruptedException {
        TestConnection connection = getRabbitMQTestConnection();
        TestChannel channel = getRabbitMQTestChannel();
        connection.setChannelSupplier(() -> channel);

        WriterInitContext context = new TestSinkInitContext();
        Counter numBytesOutCounter = context.metricGroup().getNumBytesSendCounter();
        Counter numRecordsSendCounter = context.metricGroup().getNumRecordsSendCounter();
        RabbitMQSinkWriter<String> writer = getDefaultWriter(context, null);
        writer.initializeConnection(connection);

        String message = "test-message";
        writeMessageToWriter(writer, message, channel, false);
        long recordsSentBeforeDelivery = numRecordsSendCounter.getCount();
        long bytesSentBeforeDelivery = numBytesOutCounter.getCount();

        channel.deliverMessage(message, true);
        writer.flush(false);

        Assertions.assertThat(recordsSentBeforeDelivery).isEqualTo(0);
        Assertions.assertThat(bytesSentBeforeDelivery).isEqualTo(0);
        Assertions.assertThat(numRecordsSendCounter.getCount()).isEqualTo(1);
        Assertions.assertThat(numBytesOutCounter.getCount()).isEqualTo(message.length());
    }

    @Test
    void writeMessageDoesNotBlockBeforeMaximumInflight() throws IOException, InterruptedException {
        TestConnection connection = getRabbitMQTestConnection();
        TestChannel channel = getRabbitMQTestChannel();
        connection.setChannelSupplier(() -> channel);

        RabbitMQSinkWriter<String> writer =
                getDefaultWriter(new TestSinkInitContextAnyThreadMailbox(), 2, null);
        writer.initializeConnection(connection);
        String firstMessage = "first-message";
        String secondMessage = "second-message";
        CountDownLatch latch1 = new CountDownLatch(1);
        CountDownLatch hasStarted1 = new CountDownLatch(1);
        CountDownLatch latch2 = new CountDownLatch(1);
        CountDownLatch hasStarted2 = new CountDownLatch(1);

        writeMessageToWriterAsync(writer, firstMessage, channel, hasStarted1, latch1);
        writeMessageToWriterAsync(writer, secondMessage, channel, hasStarted2, latch2);
        Boolean firstMessageAttemptSent =
                hasStarted1.await(1, java.util.concurrent.TimeUnit.SECONDS);
        Boolean secondMessageAttemptSent =
                hasStarted2.await(1, java.util.concurrent.TimeUnit.SECONDS);
        Boolean firstMessageSent = latch1.await(1, java.util.concurrent.TimeUnit.SECONDS);
        Boolean secondMessageSent = latch2.await(1, java.util.concurrent.TimeUnit.SECONDS);

        channel.deliverAllMessages(true);
        writer.flush(false);

        Assertions.assertThat(firstMessageAttemptSent).isTrue();
        Assertions.assertThat(secondMessageAttemptSent).isTrue();
        Assertions.assertThat(firstMessageSent).isTrue();
        Assertions.assertThat(secondMessageSent).isTrue();
        Assertions.assertThat(channel.getDeliveredMessages())
                .containsExactlyInAnyOrder(firstMessage, secondMessage);
    }

    @Test
    void writeMessageBlocksAfterMaximumInflight() throws IOException, InterruptedException {
        TestConnection connection = getRabbitMQTestConnection();
        TestChannel channel = getRabbitMQTestChannel();
        connection.setChannelSupplier(() -> channel);

        RabbitMQSinkWriter<String> writer =
                getDefaultWriter(new TestSinkInitContextAnyThreadMailbox(), 1, null);
        writer.initializeConnection(connection);
        String firstMessage = "first-message";
        String secondMessage = "second-message";
        CountDownLatch latch1 = new CountDownLatch(1);
        CountDownLatch hasStarted1 = new CountDownLatch(1);
        CountDownLatch latch2 = new CountDownLatch(1);
        CountDownLatch hasStarted2 = new CountDownLatch(1);

        writeMessageToWriterAsync(writer, firstMessage, channel, hasStarted1, latch1);
        Boolean firstMessageAttemptSent =
                hasStarted1.await(1, java.util.concurrent.TimeUnit.SECONDS);
        Boolean firstMessageSent = latch1.await(1, java.util.concurrent.TimeUnit.SECONDS);

        writeMessageToWriterAsync(writer, secondMessage, channel, hasStarted2, latch2);
        Boolean secondMessageAttemptSent =
                hasStarted2.await(1, java.util.concurrent.TimeUnit.SECONDS);
        Boolean secondMessageBlocked = !latch2.await(1, java.util.concurrent.TimeUnit.SECONDS);

        channel.deliverMessage(firstMessage, true);

        Boolean secondMessageSent = latch2.await(1, java.util.concurrent.TimeUnit.SECONDS);

        channel.deliverMessage(secondMessage, true);
        writer.flush(false);

        Assertions.assertThat(firstMessageAttemptSent).isTrue();
        Assertions.assertThat(secondMessageAttemptSent).isTrue();
        Assertions.assertThat(firstMessageSent).isTrue();
        Assertions.assertThat(secondMessageBlocked).isTrue();
        Assertions.assertThat(secondMessageSent).isTrue();
        Assertions.assertThat(channel.getDeliveredMessages())
                .containsExactlyInAnyOrder(firstMessage, secondMessage);
    }

    @Test
    void writeWithExchangeSupportedConverter() throws IOException, InterruptedException {
        TestConnection connection = getRabbitMQTestConnection();
        TestChannel channel = getRabbitMQTestChannel();
        connection.setChannelSupplier(() -> channel);
        TestMessageConverter messageConverter = new TestMessageConverter();
        messageConverter.setExchange("test-exchange");
        SerializableReturnListener returnListener = (i, s, s1, s2, basicProperties, bytes) -> {};

        RabbitMQSinkWriter<String> writer =
                new RabbitMQSinkWriter<>(
                        new TestSinkInitContext(),
                        QUEUE_NAME,
                        messageConverter,
                        new SimpleStringSchema(),
                        returnListener,
                        MAXIMUM_INFLIGHT_MESSAGES,
                        false);
        writer.initializeConnection(connection);

        // write first message
        String firstMessage = "test-message-1";
        messageConverter.setRoutingKey("test-routing-key-1");
        messageConverter.setMarkNextMessageAsImmediate(true);
        writeMessageToWriter(writer, firstMessage, channel);

        // write second message
        String secondMessage = "test-message-2";
        AMQP.BasicProperties secondBasicProperties =
                new AMQP.BasicProperties.Builder().appId("appId-2").build();
        messageConverter.setRoutingKey("test-routing-key-2");
        messageConverter.setMarkNextMessageAsImmediate(false);
        messageConverter.setMarkNextMessageAsMandatory(true);
        messageConverter.setProperties(secondBasicProperties);
        writeMessageToWriter(writer, secondMessage, channel);

        // write third message
        String thirdMessage = "test-message-3";
        AMQP.BasicProperties thirdBasicProperties =
                new AMQP.BasicProperties.Builder().appId("appId-3").build();
        messageConverter.setRoutingKey("test-routing-key-3");
        messageConverter.setMarkNextMessageAsImmediate(true);
        messageConverter.setMarkNextMessageAsMandatory(true);
        messageConverter.setProperties(thirdBasicProperties);
        writeMessageToWriter(writer, thirdMessage, channel);

        writer.flush(false);

        Assertions.assertThat(channel.getPublishedMessageArguments(firstMessage))
                .isEqualTo(Tuple5.of("test-exchange", "test-routing-key-1", false, true, null));

        Assertions.assertThat(channel.getPublishedMessageArguments(secondMessage))
                .isEqualTo(
                        Tuple5.of(
                                "test-exchange",
                                "test-routing-key-2",
                                true,
                                false,
                                secondBasicProperties));

        Assertions.assertThat(channel.getPublishedMessageArguments(thirdMessage))
                .isEqualTo(
                        Tuple5.of(
                                "test-exchange",
                                "test-routing-key-3",
                                true,
                                true,
                                thirdBasicProperties));

        Assertions.assertThat(channel.getDeliveredMessages())
                .containsExactlyInAnyOrder(firstMessage, secondMessage, thirdMessage);
    }

    @Test
    void writeWithExchangeSupportedConverterFailsIfReturnListenerIsNull() throws IOException {
        TestConnection connection = getRabbitMQTestConnection();
        TestChannel channel = getRabbitMQTestChannel();
        connection.setChannelSupplier(() -> channel);
        TestMessageConverter messageConverter = new TestMessageConverter();
        messageConverter.setExchange("test-exchange");
        messageConverter.setMarkNextMessageAsImmediate(true);
        RabbitMQSinkWriter<String> writer =
                new RabbitMQSinkWriter<>(
                        new TestSinkInitContext(),
                        QUEUE_NAME,
                        messageConverter,
                        new SimpleStringSchema(),
                        null,
                        MAXIMUM_INFLIGHT_MESSAGES,
                        false);
        writer.initializeConnection(connection);

        Assertions.assertThatIllegalArgumentException()
                .isThrownBy(() -> writeMessageToWriter(writer, "test-message", channel))
                .withMessageContaining(
                        "Return listener must be set if immediate or mandatory delivery is requested");
    }

    @Test
    void writerRetriesOnFailureIfFailOnErrorIsUnset() throws IOException, InterruptedException {
        TestConnection connection = getRabbitMQTestConnection();
        TestChannel channel = getRabbitMQTestChannel();
        connection.setChannelSupplier(() -> channel);

        RabbitMQSinkWriter<String> writer = getDefaultWriter(null);
        writer.initializeConnection(connection);
        String message = "fail-message";
        writeMessageToWriter(writer, message, channel);
        writer.flush(false);

        Assertions.assertThat(channel.getDeliveredMessages()).containsExactly(message);
    }

    @Test
    void writerFailsOnErrorIfFailOnErrorIsSet() throws IOException, InterruptedException {
        TestConnection connection = getRabbitMQTestConnection();
        TestChannel channel = getRabbitMQTestChannel();
        connection.setChannelSupplier(() -> channel);

        RabbitMQSinkWriter<String> writer =
                new RabbitMQSinkWriter<>(
                        new TestSinkInitContext(),
                        QUEUE_NAME,
                        new DefaultRabbitMQMessageConverter<>(),
                        new SimpleStringSchema(),
                        null,
                        MAXIMUM_INFLIGHT_MESSAGES,
                        true);

        writer.initializeConnection(connection);
        String message = "fail-message";
        writeMessageToWriter(writer, message, channel);

        Assertions.assertThatExceptionOfType(FlinkRuntimeException.class)
                .isThrownBy(() -> writer.flush(false))
                .withMessage(
                        "Failed to send message with sequence number 0 and payload fail-message");
    }

    @Test
    void closeWriterClosesChannelAndConnection() throws Exception {
        TestConnection connection = getRabbitMQTestConnection();
        TestChannel channel = getRabbitMQTestChannel();
        connection.setChannelSupplier(() -> channel);

        RabbitMQSinkWriter<String> writer = getDefaultWriter(null);
        writer.initializeConnection(connection);
        writer.close();

        Assertions.assertThat(channel.isOpen()).isFalse();
        Assertions.assertThat(connection.isOpen()).isFalse();
    }

    private void writeMessageToWriter(
            RabbitMQSinkWriter<String> writer, String message, TestChannel channel)
            throws IOException, InterruptedException {
        writeMessageToWriter(writer, message, channel, true);
    }

    private void writeMessageToWriter(
            RabbitMQSinkWriter<String> writer, String message, TestChannel channel, boolean deliver)
            throws IOException, InterruptedException {
        channel.prepareMessage(message);
        writer.write(message, null);
        if (deliver) {
            channel.deliverMessage(message);
        }
    }

    private void writeMessageToWriterAsync(
            RabbitMQSinkWriter<String> writer,
            String message,
            TestChannel channel,
            CountDownLatch hasStarted,
            CountDownLatch latch)
            throws IOException, InterruptedException {
        Thread thread =
                new Thread(
                        () -> {
                            hasStarted.countDown();
                            try {
                                writeMessageToWriter(writer, message, channel, false);
                            } catch (IOException | InterruptedException ignored) {
                            }
                            latch.countDown();
                        });
        thread.start();
    }

    private TestConnection getRabbitMQTestConnection() {
        return new TestConnection(getRabbitMQConnectionConfig());
    }

    private TestChannel getRabbitMQTestChannel() {
        return new TestChannel((byte[] bytes) -> new String(bytes).startsWith("fail"));
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

    private RabbitMQSinkWriter<String> getDefaultWriter(SerializableReturnListener returnListener) {
        return new RabbitMQSinkWriter<>(
                new TestSinkInitContext(),
                QUEUE_NAME,
                new DefaultRabbitMQMessageConverter<>(),
                new SimpleStringSchema(),
                returnListener,
                MAXIMUM_INFLIGHT_MESSAGES,
                false);
    }

    private RabbitMQSinkWriter<String> getDefaultWriter(
            WriterInitContext context, SerializableReturnListener returnListener) {
        return new RabbitMQSinkWriter<>(
                context,
                QUEUE_NAME,
                new DefaultRabbitMQMessageConverter<>(),
                new SimpleStringSchema(),
                returnListener,
                MAXIMUM_INFLIGHT_MESSAGES,
                false);
    }

    private RabbitMQSinkWriter<String> getDefaultWriter(
            WriterInitContext context,
            int maximumInflightMessages,
            SerializableReturnListener returnListener) {
        return new RabbitMQSinkWriter<>(
                context,
                QUEUE_NAME,
                new DefaultRabbitMQMessageConverter<>(),
                new SimpleStringSchema(),
                returnListener,
                maximumInflightMessages,
                false);
    }
}
