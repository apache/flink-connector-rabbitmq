package org.apache.flink.connector.rabbitmq.common;

import org.apache.flink.annotation.PublicEvolving;

import java.io.Serializable;

/**
 * Interface for converting messages to {@link RabbitMQMessage}.
 *
 * @param <T> type of the message to be converted
 */
@PublicEvolving
public interface RabbitMQMessageConverter<T> extends Serializable {

    RabbitMQMessage<T> toRabbitMQMessage(T value);

    boolean supportsExchangeRouting();
}
