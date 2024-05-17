package org.apache.flink.connector.rabbitmq.common;

import org.apache.flink.annotation.PublicEvolving;

import com.rabbitmq.client.AMQP.BasicProperties;

import javax.annotation.Nullable;

import java.io.Serializable;

/**
 * A message to be sent to RabbitMQ with publish options.
 *
 * @param <T> type of the message to be sent
 */
@PublicEvolving
public class RabbitMQMessage<T> implements Serializable {
    private static final long serialVersionUID = 1L;

    private final T message;

    private final boolean isMandatory;

    private final boolean isImmediate;

    @Nullable private final String routingKey;

    @Nullable private final String exchange;

    private final BasicProperties messageProperties;

    private Long serializedSize;

    private RabbitMQMessage(Builder<T> builder) {
        this(
                builder.message,
                builder.routingKey,
                builder.exchange,
                builder.messageProperties,
                builder.isMandatory,
                builder.isImmediate);
    }

    private RabbitMQMessage(
            T message,
            @Nullable String routingKey,
            @Nullable String exchange,
            BasicProperties messageProperties,
            boolean isMandatory,
            boolean isImmediate) {
        this.message = message;
        this.routingKey = routingKey;
        this.exchange = exchange;
        this.messageProperties = messageProperties;
        this.isMandatory = isMandatory;
        this.isImmediate = isImmediate;
    }

    public T getMessage() {
        return message;
    }

    @Nullable
    public String getRoutingKey() {
        return routingKey;
    }

    @Nullable
    public String getExchange() {
        return exchange;
    }

    public BasicProperties getMessageProperties() {
        return messageProperties;
    }

    public boolean isMandatory() {
        return isMandatory;
    }

    public boolean isImmediate() {
        return isImmediate;
    }

    public Long getSerializedSize() {
        return serializedSize;
    }

    public void setSerializedSize(Long serializedSize) {
        this.serializedSize = serializedSize;
    }

    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    /**
     * Builder for {@link RabbitMQMessage}.
     *
     * @param <T> type of the message to be sent
     */
    public static class Builder<T> {
        private T message;
        private boolean isMandatory = false;
        private boolean isImmediate = false;
        private String routingKey;
        private String exchange;
        private BasicProperties messageProperties;

        public Builder<T> setMessage(T message) {
            this.message = message;
            return this;
        }

        public Builder<T> setMandatory(boolean mandatory) {
            isMandatory = mandatory;
            return this;
        }

        public Builder<T> setImmediate(boolean immediate) {
            isImmediate = immediate;
            return this;
        }

        public Builder<T> setRoutingKey(String routingKey) {
            this.routingKey = routingKey;
            return this;
        }

        public Builder<T> setExchange(String exchange) {
            this.exchange = exchange;
            return this;
        }

        public Builder<T> setMessageProperties(BasicProperties messageProperties) {
            this.messageProperties = messageProperties;
            return this;
        }

        public RabbitMQMessage<T> build() {
            return new RabbitMQMessage<>(this);
        }
    }
}
