/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.rabbitmq.common;

import org.apache.flink.connector.rabbitmq.source.RabbitMQSource;
import org.apache.flink.util.Preconditions;

import com.rabbitmq.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;

import static java.util.Objects.requireNonNull;

/**
 * This class is copied from the previous RabbitMQ connector. Connection Configuration for RMQ. If
 * {@link Builder#setUri(String)} has been set then {@link
 * RabbitMQConnectionConfig#RabbitMQConnectionConfig(String, Integer, Boolean, Boolean, Integer,
 * Integer, Integer, Integer, Integer)} will be used to initialize the RMQ connection or {@link
 * RabbitMQConnectionConfig#RabbitMQConnectionConfig(String, Integer, String, String, String,
 * Integer, Boolean, Boolean, Integer, Integer, Integer, Integer, Integer)} will be used to
 * initialize the RMQ connection.
 */
public class RabbitMQConnectionConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(RabbitMQConnectionConfig.class);

    private String host;
    private Integer port;
    private String virtualHost;
    private String username;
    private String password;
    private String uri;

    private final @Nullable Integer networkRecoveryInterval;
    private final @Nullable Boolean automaticRecovery;
    private final @Nullable Boolean topologyRecovery;

    private final @Nullable Integer connectionTimeout;
    private final @Nullable Integer requestedChannelMax;
    private final @Nullable Integer requestedFrameMax;
    private final @Nullable Integer requestedHeartbeat;

    private final @Nullable Integer prefetchCount;

    /**
     * @param host host name
     * @param port port
     * @param virtualHost virtual host
     * @param username username
     * @param password password
     * @param networkRecoveryInterval connection recovery interval in milliseconds
     * @param automaticRecovery if automatic connection recovery
     * @param topologyRecovery if topology recovery
     * @param connectionTimeout connection timeout
     * @param requestedChannelMax requested maximum channel number
     * @param requestedFrameMax requested maximum frame size
     * @param requestedHeartbeat requested heartbeat interval
     * @throws NullPointerException if host or virtual host or username or password is null
     */
    private RabbitMQConnectionConfig(
            String host,
            Integer port,
            String virtualHost,
            String username,
            String password,
            Integer networkRecoveryInterval,
            Boolean automaticRecovery,
            Boolean topologyRecovery,
            Integer connectionTimeout,
            Integer requestedChannelMax,
            Integer requestedFrameMax,
            Integer requestedHeartbeat,
            Integer prefetchCount) {
        this.host = requireNonNull(host);
        this.port = requireNonNull(port);
        this.virtualHost = requireNonNull(virtualHost);
        this.username = requireNonNull(username);
        this.password = requireNonNull(password);

        this.networkRecoveryInterval = networkRecoveryInterval;
        this.automaticRecovery = automaticRecovery;
        this.topologyRecovery = topologyRecovery;
        this.connectionTimeout = connectionTimeout;
        this.requestedChannelMax = requestedChannelMax;
        this.requestedFrameMax = requestedFrameMax;
        this.requestedHeartbeat = requestedHeartbeat;
        this.prefetchCount = prefetchCount;
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
     * @throws NullPointerException if URI is null
     */
    private RabbitMQConnectionConfig(
            String uri,
            Integer networkRecoveryInterval,
            Boolean automaticRecovery,
            Boolean topologyRecovery,
            Integer connectionTimeout,
            Integer requestedChannelMax,
            Integer requestedFrameMax,
            Integer requestedHeartbeat,
            Integer prefetchCount) {

        this.uri = Preconditions.checkNotNull(uri, "Uri can not be null");

        this.networkRecoveryInterval = networkRecoveryInterval;
        this.automaticRecovery = automaticRecovery;
        this.topologyRecovery = topologyRecovery;
        this.connectionTimeout = connectionTimeout;
        this.requestedChannelMax = requestedChannelMax;
        this.requestedFrameMax = requestedFrameMax;
        this.requestedHeartbeat = requestedHeartbeat;
        this.prefetchCount = prefetchCount;
    }

    /** @return the host to use for connections */
    public String getHost() {
        return host;
    }

    /** @return the port to use for connections */
    public int getPort() {
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
    public @Nullable Integer getNetworkRecoveryInterval() {
        return networkRecoveryInterval;
    }

    /**
     * Returns true if automatic connection recovery is enabled, false otherwise.
     *
     * @return true if automatic connection recovery is enabled, false otherwise
     */
    public @Nullable Boolean isAutomaticRecovery() {
        return automaticRecovery;
    }

    /**
     * Returns true if topology recovery is enabled, false otherwise.
     *
     * @return true if topology recovery is enabled, false otherwise
     */
    public @Nullable Boolean isTopologyRecovery() {
        return topologyRecovery;
    }

    /**
     * Retrieve the connection timeout.
     *
     * @return the connection timeout, in milliseconds; zero for infinite
     */
    public @Nullable Integer getConnectionTimeout() {
        return connectionTimeout;
    }

    /**
     * Retrieve the requested maximum channel number.
     *
     * @return the initially requested maximum channel number; zero for unlimited
     */
    public @Nullable Integer getRequestedChannelMax() {
        return requestedChannelMax;
    }

    /**
     * Retrieve the requested maximum frame size.
     *
     * @return the initially requested maximum frame size, in octets; zero for unlimited
     */
    public @Nullable Integer getRequestedFrameMax() {
        return requestedFrameMax;
    }

    /**
     * Retrieve the requested heartbeat interval.
     *
     * @return the initially requested heartbeat interval, in seconds; zero for none
     */
    public @Nullable Integer getRequestedHeartbeat() {
        return requestedHeartbeat;
    }

    /**
     * Retrieve the the channel prefetch count.
     *
     * @return an integer that represents the prefetch count, if not null, for the consumer channel
     */
    public @Nullable Integer getPrefetchCount() {
        return prefetchCount;
    }

    /**
     * @return Connection Factory for RMQ
     * @throws URISyntaxException if Malformed URI has been passed
     * @throws NoSuchAlgorithmException if the ssl factory could not be created
     * @throws KeyManagementException if the ssl context could not be initialized
     */
    public ConnectionFactory getConnectionFactory()
            throws URISyntaxException, NoSuchAlgorithmException, KeyManagementException {
        ConnectionFactory factory = new ConnectionFactory();
        if (this.uri != null && !this.uri.isEmpty()) {
            try {
                factory.setUri(this.uri);
            } catch (URISyntaxException e) {
                LOG.error("Failed to parse uri", e);
                throw e;
            } catch (KeyManagementException e) {
                // this should never happen
                LOG.error("Failed to initialize ssl context.", e);
                throw e;
            } catch (NoSuchAlgorithmException e) {
                // this should never happen
                LOG.error("Failed to setup ssl factory.", e);
                throw e;
            }
        } else {
            factory.setHost(this.host);
            factory.setPort(this.port);
            factory.setVirtualHost(this.virtualHost);
            factory.setUsername(this.username);
            factory.setPassword(this.password);
        }

        if (this.automaticRecovery != null) {
            factory.setAutomaticRecoveryEnabled(this.automaticRecovery);
        }
        if (this.connectionTimeout != null) {
            factory.setConnectionTimeout(this.connectionTimeout);
        }
        if (this.networkRecoveryInterval != null) {
            factory.setNetworkRecoveryInterval(this.networkRecoveryInterval);
        }
        if (this.requestedHeartbeat != null) {
            factory.setRequestedHeartbeat(this.requestedHeartbeat);
        }
        if (this.topologyRecovery != null) {
            factory.setTopologyRecoveryEnabled(this.topologyRecovery);
        }
        if (this.requestedChannelMax != null) {
            factory.setRequestedChannelMax(this.requestedChannelMax);
        }
        if (this.requestedFrameMax != null) {
            factory.setRequestedFrameMax(this.requestedFrameMax);
        }

        return factory;
    }

    /** The Builder Class for {@link RabbitMQConnectionConfig}. */
    public static class Builder {

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

        private String uri;

        /**
         * Set the target port.
         *
         * @param port the default port to use for connections
         * @return the Builder
         */
        public Builder setPort(int port) {
            this.port = port;
            return this;
        }

        /**
         * @param host the default host to use for connections
         * @return the Builder
         */
        public Builder setHost(String host) {
            this.host = host;
            return this;
        }

        /**
         * Set the virtual host.
         *
         * @param virtualHost the virtual host to use when connecting to the broker
         * @return the Builder
         */
        public Builder setVirtualHost(String virtualHost) {
            this.virtualHost = virtualHost;
            return this;
        }

        /**
         * Set the user name.
         *
         * @param username the AMQP user name to use when connecting to the broker
         * @return the Builder
         */
        public Builder setUserName(String username) {
            this.username = username;
            return this;
        }

        /**
         * Set the password.
         *
         * @param password the password to use when connecting to the broker
         * @return the Builder
         */
        public Builder setPassword(String password) {
            this.password = password;
            return this;
        }

        /**
         * Convenience method for setting the fields in an AMQP URI: host, port, username, password
         * and virtual host. If any part of the URI is omitted, the ConnectionFactory's
         * corresponding variable is left unchanged.
         *
         * @param uri is the AMQP URI containing the data
         * @return the Builder
         */
        public Builder setUri(String uri) {
            this.uri = uri;
            return this;
        }

        /**
         * Enables or disables topology recovery.
         *
         * @param topologyRecovery if true, enables topology recovery
         * @return the Builder
         */
        public Builder setTopologyRecoveryEnabled(boolean topologyRecovery) {
            this.topologyRecovery = topologyRecovery;
            return this;
        }

        /**
         * Set the requested heartbeat.
         *
         * @param requestedHeartbeat the initially requested heartbeat interval, in seconds; zero
         *     for none
         * @return the Builder
         */
        public Builder setRequestedHeartbeat(int requestedHeartbeat) {
            this.requestedHeartbeat = requestedHeartbeat;
            return this;
        }

        /**
         * Set the requested maximum frame size.
         *
         * @param requestedFrameMax initially requested maximum frame size, in octets; zero for
         *     unlimited
         * @return the Builder
         */
        public Builder setRequestedFrameMax(int requestedFrameMax) {
            this.requestedFrameMax = requestedFrameMax;
            return this;
        }

        /**
         * Set the requested maximum channel number.
         *
         * @param requestedChannelMax initially requested maximum channel number; zero for unlimited
         */
        public Builder setRequestedChannelMax(int requestedChannelMax) {
            this.requestedChannelMax = requestedChannelMax;
            return this;
        }

        /**
         * Sets connection recovery interval. Default is 5000.
         *
         * @param networkRecoveryInterval how long will automatic recovery wait before attempting to
         *     reconnect, in ms
         * @return the Builder
         */
        public Builder setNetworkRecoveryInterval(int networkRecoveryInterval) {
            this.networkRecoveryInterval = networkRecoveryInterval;
            return this;
        }

        /**
         * Set the connection timeout.
         *
         * @param connectionTimeout connection establishment timeout in milliseconds; zero for
         *     infinite
         * @return the Builder
         */
        public Builder setConnectionTimeout(int connectionTimeout) {
            this.connectionTimeout = connectionTimeout;
            return this;
        }

        /**
         * Enables or disables automatic connection recovery.
         *
         * @param automaticRecovery if true, enables connection recovery
         * @return the Builder
         */
        public Builder setAutomaticRecovery(boolean automaticRecovery) {
            this.automaticRecovery = automaticRecovery;
            return this;
        }

        /**
         * Enables setting basicQos for the consumer channel. Only applicable to the {@link
         * RabbitMQSource}. Set to 0 for unlimited, which is the default.
         *
         * @see <a href="https://www.rabbitmq.com/consumer-prefetch.html">Consumer Prefetch</a>
         * @see <a href="https://www.rabbitmq.com/confirms.html#channel-qos-prefetch">Channel
         *     Prefetch (QoS)</a>
         * @param prefetchCount the max number of messages to receive without acknowledgement.
         * @return the Builder
         */
        public Builder setPrefetchCount(int prefetchCount) {
            this.prefetchCount = prefetchCount;
            return this;
        }

        /**
         * The Builder method.
         *
         * <p>If URI is NULL we use host, port, vHost, username, password combination to initialize
         * connection. using {@link RabbitMQConnectionConfig#RabbitMQConnectionConfig(String,
         * Integer, String, String, String, Integer, Boolean, Boolean, Integer, Integer, Integer,
         * Integer, Integer)}.
         *
         * <p>Otherwise the URI will be used to initialize the client connection {@link
         * RabbitMQConnectionConfig#RabbitMQConnectionConfig(String, Integer, Boolean, Boolean,
         * Integer, Integer, Integer, Integer, Integer)}
         *
         * @return RMQConnectionConfig
         */
        public RabbitMQConnectionConfig build() {
            if (this.uri != null) {
                return new RabbitMQConnectionConfig(
                        this.uri,
                        this.networkRecoveryInterval,
                        this.automaticRecovery,
                        this.topologyRecovery,
                        this.connectionTimeout,
                        this.requestedChannelMax,
                        this.requestedFrameMax,
                        this.requestedHeartbeat,
                        this.prefetchCount);
            } else {
                return new RabbitMQConnectionConfig(
                        this.host,
                        this.port,
                        this.virtualHost,
                        this.username,
                        this.password,
                        this.networkRecoveryInterval,
                        this.automaticRecovery,
                        this.topologyRecovery,
                        this.connectionTimeout,
                        this.requestedChannelMax,
                        this.requestedFrameMax,
                        this.requestedHeartbeat,
                        this.prefetchCount);
            }
        }
    }
}
