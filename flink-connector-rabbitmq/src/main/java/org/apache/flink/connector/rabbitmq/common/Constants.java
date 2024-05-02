package org.apache.flink.connector.rabbitmq.common;

import org.apache.flink.annotation.PublicEvolving;

/** Constants for the RabbitMQ connector. */
@PublicEvolving
public class Constants {

    /** The default RabbitMQ host Exchange used when exchange routing is disabled. */
    public static final String DEFAULT_EXCHANGE = "";

    /** The default maximum number of inflight messages handled by SinkWriter at the same time. */
    public static final int DEFAULT_MAX_INFLIGHT = 100;

    /** The default behaviour of sink on failing to send elements. */
    public static final boolean DEFAULT_FAIL_ON_ERROR = false;
}
