package org.apache.flink.connector.rabbitmq.common;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests for {@link DefaultRabbitMQMessageConverter}. */
public class DefaultRabbitMQMessageConverterTest {
    @Test
    void defaultRabbitMQMessageConverterDoesNotSupportExchangeRouting() {
        DefaultRabbitMQMessageConverter<Integer> messageConverter =
                new DefaultRabbitMQMessageConverter<>();
        Assertions.assertThat(messageConverter.supportsExchangeRouting()).isFalse();
    }

    @Test
    public void convertToRabbitMQMessage() {
        DefaultRabbitMQMessageConverter<Integer> messageConverter =
                new DefaultRabbitMQMessageConverter<>();
        RabbitMQMessage<Integer> rabbitMQMessage = messageConverter.toRabbitMQMessage(1);
        Assertions.assertThat(rabbitMQMessage.getMessage()).isEqualTo(1);
        Assertions.assertThat(rabbitMQMessage.getRoutingKey()).isNull();
        Assertions.assertThat(rabbitMQMessage.getExchange()).isEqualTo(Constants.DEFAULT_EXCHANGE);
        Assertions.assertThat(rabbitMQMessage.getMessageProperties()).isNull();
        Assertions.assertThat(rabbitMQMessage.isMandatory()).isFalse();
        Assertions.assertThat(rabbitMQMessage.isImmediate()).isFalse();
    }
}
