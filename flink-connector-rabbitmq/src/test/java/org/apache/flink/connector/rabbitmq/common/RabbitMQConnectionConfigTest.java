package org.apache.flink.connector.rabbitmq.common;

import org.apache.flink.connector.rabbitmq.common.util.RabbitMQConnectionUtil;

import com.rabbitmq.client.ConnectionFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

/** Tests for the {@link RabbitMQConnectionConfig}. */
public class RabbitMQConnectionConfigTest {
    @Test
    void shouldThrowNullPointExceptionIfHostIsNull() {
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () ->
                        RabbitMQConnectionConfig.builder()
                                .setPort(1000)
                                .setUserName("guest")
                                .setPassword("guest")
                                .setVirtualHost("/")
                                .build(),
                "Either URI or host/port must be set");
    }

    @Test
    void shouldThrowNullPointExceptionIfPortIsNull() throws RabbitMQConnectionSetupException {
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () ->
                        RabbitMQConnectionConfig.builder()
                                .setHost("localhost")
                                .setUserName("guest")
                                .setPassword("guest")
                                .setVirtualHost("/")
                                .build(),
                "Either URI or host/port must be set");
    }

    @Test
    void shouldSetDefaultValueIfConnectionTimeoutNotGiven()
            throws RabbitMQConnectionSetupException {
        RabbitMQConnectionConfig connectionConfig =
                RabbitMQConnectionConfig.builder()
                        .setHost("localhost")
                        .setPort(5672)
                        .setUserName("guest")
                        .setPassword("guest")
                        .setVirtualHost("/")
                        .build();
        Assertions.assertEquals(
                ConnectionFactory.DEFAULT_CONNECTION_TIMEOUT,
                RabbitMQConnectionUtil.getConnectionFactory(connectionConfig)
                        .getConnectionTimeout());
    }

    @Test
    void shouldSetProvidedValueIfConnectionTimeoutGiven() throws RabbitMQConnectionSetupException {
        int connectionTimeout = 1000;
        RabbitMQConnectionConfig connectionConfig =
                RabbitMQConnectionConfig.builder()
                        .setHost("localhost")
                        .setUserName("guest")
                        .setPort(5672)
                        .setPassword("guest")
                        .setVirtualHost("/")
                        .setConnectionTimeout(connectionTimeout)
                        .build();
        Assertions.assertEquals(
                connectionTimeout,
                RabbitMQConnectionUtil.getConnectionFactory(connectionConfig)
                        .getConnectionTimeout());
    }

    @Test
    void shouldSetOptionalPrefetchCount() {
        int prefetchCount = 100;
        RabbitMQConnectionConfig connectionConfig =
                RabbitMQConnectionConfig.builder()
                        .setHost("localhost")
                        .setPort(5000)
                        .setUserName("guest")
                        .setPassword("guest")
                        .setVirtualHost("/")
                        .setPrefetchCount(prefetchCount)
                        .build();
        Assertions.assertTrue(connectionConfig.getPrefetchCount().isPresent());
        Assertions.assertEquals(prefetchCount, connectionConfig.getPrefetchCount().get());
    }

    @Test
    void shouldNotSetOptional() {
        RabbitMQConnectionConfig connectionConfig =
                RabbitMQConnectionConfig.builder()
                        .setHost("localhost")
                        .setPort(5000)
                        .setUserName("guest")
                        .setPassword("guest")
                        .setVirtualHost("/")
                        .build();
        Assertions.assertFalse(connectionConfig.getPrefetchCount().isPresent());
    }

    @Test
    void shouldSetDeliveryTimeout() {
        int deliveryTimeout = 1000;
        RabbitMQConnectionConfig connectionConfig =
                RabbitMQConnectionConfig.builder()
                        .setHost("localhost")
                        .setPort(5000)
                        .setUserName("guest")
                        .setPassword("guest")
                        .setVirtualHost("/")
                        .setDeliveryTimeout(deliveryTimeout)
                        .build();
        Assertions.assertEquals(deliveryTimeout, connectionConfig.getDeliveryTimeout());
    }

    @Test
    void shouldSetDeliveryTimeoutWithTimeUnit() {
        int deliveryTimeout = 1000;
        RabbitMQConnectionConfig connectionConfig =
                RabbitMQConnectionConfig.builder()
                        .setHost("localhost")
                        .setPort(5000)
                        .setUserName("guest")
                        .setPassword("guest")
                        .setVirtualHost("/")
                        .setDeliveryTimeout(deliveryTimeout, TimeUnit.MILLISECONDS)
                        .build();
        Assertions.assertEquals(deliveryTimeout, connectionConfig.getDeliveryTimeout());
    }

    @Test
    void shouldReturnDefaultDeliveryTimeout() {
        RabbitMQConnectionConfig connectionConfig =
                RabbitMQConnectionConfig.builder()
                        .setHost("localhost")
                        .setPort(5000)
                        .setUserName("guest")
                        .setPassword("guest")
                        .setVirtualHost("/")
                        .build();
        Assertions.assertEquals(30000, connectionConfig.getDeliveryTimeout());
    }
}
