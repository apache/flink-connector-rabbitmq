package org.apache.flink.connector.rabbitmq.common.util;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.rabbitmq.common.RabbitMQConnectionConfig;
import org.apache.flink.connector.rabbitmq.common.RabbitMQConnectionSetupException;

import com.rabbitmq.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;

/**
 * Utility class for creating a {@link ConnectionFactory} from a {@link RabbitMQConnectionConfig}.
 */
@Internal
public class RabbitMQConnectionUtil {
    private static final Logger LOG = LoggerFactory.getLogger(RabbitMQConnectionUtil.class);

    public static ConnectionFactory getConnectionFactory(RabbitMQConnectionConfig connectionConfig)
            throws RabbitMQConnectionSetupException {
        ConnectionFactory factory = new ConnectionFactory();
        if (connectionConfig.getUri() != null && !connectionConfig.getUri().isEmpty()) {
            try {
                factory.setUri(connectionConfig.getUri());
            } catch (URISyntaxException | KeyManagementException | NoSuchAlgorithmException e) {
                LOG.error("Failed to parse uri", e);
                throw new RabbitMQConnectionSetupException(
                        "Failed to create connection factory", e);
            }
        } else {
            factory.setHost(connectionConfig.getHost());
            factory.setPort(connectionConfig.getPort());
            factory.setVirtualHost(connectionConfig.getVirtualHost());
            factory.setUsername(connectionConfig.getUsername());
            factory.setPassword(connectionConfig.getPassword());
        }

        if (connectionConfig.isAutomaticRecovery() != null) {
            factory.setAutomaticRecoveryEnabled(connectionConfig.isAutomaticRecovery());
        }
        if (connectionConfig.getConnectionTimeout() != null) {
            factory.setConnectionTimeout(connectionConfig.getConnectionTimeout());
        }
        if (connectionConfig.getNetworkRecoveryInterval() != null) {
            factory.setNetworkRecoveryInterval(connectionConfig.getNetworkRecoveryInterval());
        }
        if (connectionConfig.getRequestedHeartbeat() != null) {
            factory.setRequestedHeartbeat(connectionConfig.getRequestedHeartbeat());
        }
        if (connectionConfig.isTopologyRecovery() != null) {
            factory.setTopologyRecoveryEnabled(connectionConfig.isTopologyRecovery());
        }
        if (connectionConfig.getRequestedChannelMax() != null) {
            factory.setRequestedChannelMax(connectionConfig.getRequestedChannelMax());
        }
        if (connectionConfig.getRequestedFrameMax() != null) {
            factory.setRequestedFrameMax(connectionConfig.getRequestedFrameMax());
        }

        return factory;
    }
}
