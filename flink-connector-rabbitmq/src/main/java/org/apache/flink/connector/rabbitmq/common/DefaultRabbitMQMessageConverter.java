package org.apache.flink.connector.rabbitmq.common;

import org.apache.flink.annotation.PublicEvolving;

import static org.apache.flink.connector.rabbitmq.common.Constants.DEFAULT_EXCHANGE;

/**
 * Default implementation of {@link RabbitMQMessageConverter}.
 *
 * @param <T> type of the message to be converted
 */
@PublicEvolving
public class DefaultRabbitMQMessageConverter<T> implements RabbitMQMessageConverter<T> {
    @Override
    public RabbitMQMessage<T> toRabbitMQMessage(T value) {
        return RabbitMQMessage.<T>builder().setMessage(value).setExchange(DEFAULT_EXCHANGE).build();
    }

    @Override
    public boolean supportsExchangeRouting() {
        return false;
    }
}
