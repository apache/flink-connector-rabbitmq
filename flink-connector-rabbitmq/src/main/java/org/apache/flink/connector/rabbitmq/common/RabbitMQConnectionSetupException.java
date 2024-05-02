package org.apache.flink.connector.rabbitmq.common;

import org.apache.flink.annotation.PublicEvolving;

import java.io.IOException;

/** Exception for RabbitMQ connection setup errors. */
@PublicEvolving
public class RabbitMQConnectionSetupException extends IOException {
    public RabbitMQConnectionSetupException(String message, Throwable cause) {
        super(message, cause);
    }
}
