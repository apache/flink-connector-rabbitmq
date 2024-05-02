package org.apache.flink.connector.rabbitmq.sink.util;

import org.apache.flink.connector.rabbitmq.common.RabbitMQMessage;
import org.apache.flink.connector.rabbitmq.common.RabbitMQMessageConverter;

import com.rabbitmq.client.AMQP;

/** Test message converter for RabbitMQ. */
public class TestMessageConverter implements RabbitMQMessageConverter<String> {

    private boolean markNextMessageAsMandatory = false;
    private boolean markNextMessageAsImmediate = false;

    private String exchange = null;

    private String routingKey = null;

    private AMQP.BasicProperties properties = null;

    public void setProperties(AMQP.BasicProperties properties) {
        this.properties = properties;
    }

    public void setExchange(String exchange) {
        this.exchange = exchange;
    }

    public void setRoutingKey(String routingKey) {
        this.routingKey = routingKey;
    }

    public void setMarkNextMessageAsMandatory(boolean markNextMessageAsMandatory) {
        this.markNextMessageAsMandatory = markNextMessageAsMandatory;
    }

    public void setMarkNextMessageAsImmediate(boolean markNextMessageAsImmediate) {
        this.markNextMessageAsImmediate = markNextMessageAsImmediate;
    }

    @Override
    public RabbitMQMessage<String> toRabbitMQMessage(String value) {
        return RabbitMQMessage.<String>builder()
                .setMessage(value)
                .setExchange(exchange)
                .setMandatory(markNextMessageAsMandatory)
                .setImmediate(markNextMessageAsImmediate)
                .setRoutingKey(routingKey)
                .setMessageProperties(properties)
                .build();
    }

    @Override
    public boolean supportsExchangeRouting() {
        return true;
    }
}
