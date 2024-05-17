package org.apache.flink.connector.rabbitmq.common;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/** Configuration class for RabbitMQ connections. */
@PublicEvolving
public class RabbitMQConnectionConfig implements Serializable {
    private static final long DEFAULT_DELIVERY_TIMEOUT = 30000;

    private String host;
    private Integer port;
    private String virtualHost;
    private String username;
    private String password;
    private String uri;

    private Integer networkRecoveryInterval;
    private Boolean automaticRecovery;
    private Boolean topologyRecovery;

    private Integer connectionTimeout;
    private Integer requestedChannelMax;
    private Integer requestedFrameMax;
    private Integer requestedHeartbeat;

    private Integer prefetchCount;
    private final long deliveryTimeout;

    protected RabbitMQConnectionConfig(RabbitMQConnectionConfig.Builder builder) {
        Preconditions.checkArgument(
                builder.uri != null || (builder.host != null && builder.port != null),
                "Either URI or host/port must be set");
        if (builder.uri == null) {
            Preconditions.checkNotNull(
                    builder.username, "username can not be null when host/port is set");
            Preconditions.checkNotNull(
                    builder.password, "password can not be null when host/port is set");
        }

        Preconditions.checkArgument(
                builder.deliveryTimeout == null || builder.deliveryTimeout > 0,
                "deliveryTimeout must be positive");
        this.uri = builder.uri;
        this.host = builder.host;
        this.port = builder.port;
        this.virtualHost = builder.virtualHost;
        this.username = builder.username;
        this.password = builder.password;

        this.networkRecoveryInterval = builder.networkRecoveryInterval;
        this.automaticRecovery = builder.automaticRecovery;
        this.topologyRecovery = builder.topologyRecovery;
        this.connectionTimeout = builder.connectionTimeout;
        this.requestedChannelMax = builder.requestedChannelMax;
        this.requestedFrameMax = builder.requestedFrameMax;
        this.requestedHeartbeat = builder.requestedHeartbeat;
        this.prefetchCount = builder.prefetchCount;
        this.deliveryTimeout =
                Optional.ofNullable(builder.deliveryTimeout).orElse(DEFAULT_DELIVERY_TIMEOUT);
    }

    /**
     * @param uri the connection URI
     * @param networkRecoveryInterval connection recovery interval in milliseconds
     * @param automaticRecovery if automatic connection recovery
     * @param topologyRecovery if topology recovery
     * @param connectionTimeout connection timeout
     * @param requestedChannelMax requested maximum channel number
     * @param requestedFrameMax requested maximum frame size
     * @param requestedHeartbeat requested heartbeat interval
     * @param deliveryTimeout message delivery timeout in the queueing consumer
     * @throws NullPointerException if URI is null
     */
    protected RabbitMQConnectionConfig(
            String uri,
            Integer networkRecoveryInterval,
            Boolean automaticRecovery,
            Boolean topologyRecovery,
            Integer connectionTimeout,
            Integer requestedChannelMax,
            Integer requestedFrameMax,
            Integer requestedHeartbeat,
            Integer prefetchCount,
            Long deliveryTimeout) {
        Preconditions.checkNotNull(uri, "Uri can not be null");
        Preconditions.checkArgument(
                deliveryTimeout == null || deliveryTimeout > 0, "deliveryTimeout must be positive");
        this.uri = uri;

        this.networkRecoveryInterval = networkRecoveryInterval;
        this.automaticRecovery = automaticRecovery;
        this.topologyRecovery = topologyRecovery;
        this.connectionTimeout = connectionTimeout;
        this.requestedChannelMax = requestedChannelMax;
        this.requestedFrameMax = requestedFrameMax;
        this.requestedHeartbeat = requestedHeartbeat;
        this.prefetchCount = prefetchCount;
        this.deliveryTimeout =
                Optional.ofNullable(deliveryTimeout).orElse(DEFAULT_DELIVERY_TIMEOUT);
    }

    /** @return the host to use for connections */
    public String getHost() {
        return host;
    }

    /** @return the port to use for connections */
    public Integer getPort() {
        return port;
    }

    /**
     * Retrieve the virtual host.
     *
     * @return the virtual host to use when connecting to the broker
     */
    public String getVirtualHost() {
        return virtualHost;
    }

    /**
     * Retrieve the user name.
     *
     * @return the AMQP user name to use when connecting to the broker
     */
    public String getUsername() {
        return username;
    }

    /**
     * Retrieve the password.
     *
     * @return the password to use when connecting to the broker
     */
    public String getPassword() {
        return password;
    }

    /**
     * Retrieve the URI.
     *
     * @return the connection URI when connecting to the broker
     */
    public String getUri() {
        return uri;
    }

    /**
     * Returns automatic connection recovery interval in milliseconds.
     *
     * @return how long will automatic recovery wait before attempting to reconnect, in ms; default
     *     is 5000
     */
    public Integer getNetworkRecoveryInterval() {
        return networkRecoveryInterval;
    }

    /**
     * Returns true if automatic connection recovery is enabled, false otherwise.
     *
     * @return true if automatic connection recovery is enabled, false otherwise
     */
    public Boolean isAutomaticRecovery() {
        return automaticRecovery;
    }

    /**
     * Returns true if topology recovery is enabled, false otherwise.
     *
     * @return true if topology recovery is enabled, false otherwise
     */
    public Boolean isTopologyRecovery() {
        return topologyRecovery;
    }

    /**
     * Retrieve the connection timeout.
     *
     * @return the connection timeout, in milliseconds; zero for infinite
     */
    public Integer getConnectionTimeout() {
        return connectionTimeout;
    }

    /**
     * Retrieve the requested maximum channel number.
     *
     * @return the initially requested maximum channel number; zero for unlimited
     */
    public Integer getRequestedChannelMax() {
        return requestedChannelMax;
    }

    /**
     * Retrieve the requested maximum frame size.
     *
     * @return the initially requested maximum frame size, in octets; zero for unlimited
     */
    public Integer getRequestedFrameMax() {
        return requestedFrameMax;
    }

    /**
     * Retrieve the requested heartbeat interval.
     *
     * @return the initially requested heartbeat interval, in seconds; zero for none
     */
    public Integer getRequestedHeartbeat() {
        return requestedHeartbeat;
    }

    /**
     * Retrieve the channel prefetch count.
     *
     * @return an Optional of the prefetch count, if set, for the consumer channel
     */
    public Optional<Integer> getPrefetchCount() {
        return Optional.ofNullable(prefetchCount);
    }

    /**
     * Retrieve the message delivery timeout used in the queueing consumer. If not specified
     * explicitly, the default value of 30000 milliseconds will be returned.
     *
     * @return the message delivery timeout, in milliseconds
     */
    public long getDeliveryTimeout() {
        return deliveryTimeout;
    }

    public static Builder<RabbitMQConnectionConfig.Builder<?>> builder() {
        return new Builder<>();
    }

    /** The Builder Class for {@link RMQConnectionConfig}. */
    public static class Builder<T extends RabbitMQConnectionConfig.Builder<?>> {

        private String host;
        private Integer port;
        private String virtualHost;
        private String username;
        private String password;

        private Integer networkRecoveryInterval;
        private Boolean automaticRecovery;
        private Boolean topologyRecovery;

        private Integer connectionTimeout;
        private Integer requestedChannelMax;
        private Integer requestedFrameMax;
        private Integer requestedHeartbeat;

        // basicQos options for consumers
        private Integer prefetchCount;

        private Long deliveryTimeout;

        private String uri;

        /**
         * Set the target port.
         *
         * @param port the default port to use for connections
         * @return the Builder
         */
        public T setPort(int port) {
            this.port = port;
            return (T) this;
        }

        /**
         * @param host the default host to use for connections
         * @return the Builder
         */
        public T setHost(String host) {
            this.host = host;
            return (T) this;
        }

        /**
         * Set the virtual host.
         *
         * @param virtualHost the virtual host to use when connecting to the broker
         * @return the Builder
         */
        public T setVirtualHost(String virtualHost) {
            this.virtualHost = virtualHost;
            return (T) this;
        }

        /**
         * Set the user name.
         *
         * @param username the AMQP user name to use when connecting to the broker
         * @return the Builder
         */
        public T setUserName(String username) {
            this.username = username;
            return (T) this;
        }

        /**
         * Set the password.
         *
         * @param password the password to use when connecting to the broker
         * @return the Builder
         */
        public T setPassword(String password) {
            this.password = password;
            return (T) this;
        }

        /**
         * Convenience method for setting the fields in an AMQP URI: host, port, username, password
         * and virtual host. If any part of the URI is omitted, the {@link
         * com.rabbitmq.client.ConnectionFactory}'s corresponding variable is left unchanged.
         *
         * @param uri is the AMQP URI containing the data
         * @return the Builder
         */
        public T setUri(String uri) {
            this.uri = uri;
            return (T) this;
        }

        /**
         * Enables or disables topology recovery.
         *
         * @param topologyRecovery if true, enables topology recovery
         * @return the Builder
         */
        public T setTopologyRecoveryEnabled(boolean topologyRecovery) {
            this.topologyRecovery = topologyRecovery;
            return (T) this;
        }

        /**
         * Set the requested heartbeat.
         *
         * @param requestedHeartbeat the initially requested heartbeat interval, in seconds; zero
         *     for none
         * @return the Builder
         */
        public T setRequestedHeartbeat(int requestedHeartbeat) {
            this.requestedHeartbeat = requestedHeartbeat;
            return (T) this;
        }

        /**
         * Set the requested maximum frame size.
         *
         * @param requestedFrameMax initially requested maximum frame size, in octets; zero for
         *     unlimited
         * @return the Builder
         */
        public T setRequestedFrameMax(int requestedFrameMax) {
            this.requestedFrameMax = requestedFrameMax;
            return (T) this;
        }

        /**
         * Set the requested maximum channel number.
         *
         * @param requestedChannelMax initially requested maximum channel number; zero for unlimited
         */
        public T setRequestedChannelMax(int requestedChannelMax) {
            this.requestedChannelMax = requestedChannelMax;
            return (T) this;
        }

        /**
         * Sets connection recovery interval. Default is 5000.
         *
         * @param networkRecoveryInterval how long will automatic recovery wait before attempting to
         *     reconnect, in ms
         * @return the Builder
         */
        public T setNetworkRecoveryInterval(int networkRecoveryInterval) {
            this.networkRecoveryInterval = networkRecoveryInterval;
            return (T) this;
        }

        /**
         * Set the connection timeout.
         *
         * @param connectionTimeout connection establishment timeout in milliseconds; zero for
         *     infinite
         * @return the Builder
         */
        public T setConnectionTimeout(int connectionTimeout) {
            this.connectionTimeout = connectionTimeout;
            return (T) this;
        }

        /**
         * Enables or disables automatic connection recovery.
         *
         * @param automaticRecovery if true, enables connection recovery
         * @return the Builder
         */
        public T setAutomaticRecovery(boolean automaticRecovery) {
            this.automaticRecovery = automaticRecovery;
            return (T) this;
        }

        /**
         * Enables setting basicQos for the consumer channel. Only applicable to the {@link
         * RMQSource}. Set to 0 for unlimited, which is the default.
         *
         * @see <a href="https://www.rabbitmq.com/consumer-prefetch.html">Consumer Prefetch</a>
         * @see <a href="https://www.rabbitmq.com/confirms.html#channel-qos-prefetch">Channel
         *     Prefetch (QoS)</a>
         * @param prefetchCount the max number of messages to receive without acknowledgement.
         * @return the Builder
         */
        public T setPrefetchCount(int prefetchCount) {
            this.prefetchCount = prefetchCount;
            return (T) this;
        }

        /**
         * Enables setting the message delivery timeout in the queueing consumer. Only applicable to
         * the {@link RMQSource}. If not set it will default to 30000.
         *
         * @param deliveryTimeout maximum wait time, in milliseconds, for the next message delivery
         * @return the Builder
         */
        public T setDeliveryTimeout(long deliveryTimeout) {
            Preconditions.checkArgument(deliveryTimeout > 0, "deliveryTimeout must be positive");
            this.deliveryTimeout = deliveryTimeout;
            return (T) this;
        }

        /**
         * Enables setting the message delivery timeout in the queueing consumer. Only applicable to
         * the {@link RMQSource}. If not set it will default to 30 seconds.
         *
         * @param deliveryTimeout maximum wait time for the next message delivery
         * @param unit deliveryTimeout unit
         * @return the Builder
         */
        public T setDeliveryTimeout(long deliveryTimeout, TimeUnit unit) {
            return setDeliveryTimeout(unit.toMillis(deliveryTimeout));
        }

        /**
         * The Builder method.
         *
         * <p>If URI is NULL we use host, port, vHost, username, password combination to initialize
         * connection. using {@link RabbitMQConnectionConfig#RabbitMQConnectionConfig(Builder)}.
         *
         * @return RMQConnectionConfig
         */
        public RabbitMQConnectionConfig build() {
            return new RabbitMQConnectionConfig(this);
        }
    }
}
