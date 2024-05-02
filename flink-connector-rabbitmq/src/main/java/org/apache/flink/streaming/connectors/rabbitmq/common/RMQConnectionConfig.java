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

package org.apache.flink.streaming.connectors.rabbitmq.common;

import org.apache.flink.connector.rabbitmq.common.RabbitMQConnectionConfig;
import org.apache.flink.connector.rabbitmq.common.RabbitMQConnectionSetupException;
import org.apache.flink.connector.rabbitmq.common.util.RabbitMQConnectionUtil;

import com.rabbitmq.client.ConnectionFactory;

import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;

/**
 * The configuration class extending {@link RabbitMQConnectionConfig} to offer backward
 * compatibility to {@link org.apache.flink.streaming.connectors.rabbitmq.RMQSink} and {@link
 * org.apache.flink.streaming.connectors.rabbitmq.RMQSource} users.
 */
public class RMQConnectionConfig extends RabbitMQConnectionConfig {

    private RMQConnectionConfig(Builder builder) {
        super(builder);
    }

    public ConnectionFactory getConnectionFactory()
            throws URISyntaxException, NoSuchAlgorithmException, KeyManagementException,
                    RabbitMQConnectionSetupException {
        try {
            return RabbitMQConnectionUtil.getConnectionFactory(this);
        } catch (RabbitMQConnectionSetupException e) {
            if (e.getCause() instanceof URISyntaxException) {
                throw (URISyntaxException) e.getCause();
            } else if (e.getCause() instanceof NoSuchAlgorithmException) {
                throw (NoSuchAlgorithmException) e.getCause();
            } else if (e.getCause() instanceof KeyManagementException) {
                throw (KeyManagementException) e.getCause();
            } else {
                throw e;
            }
        }
    }

    /** The Builder Class for {@link RMQConnectionConfig}. */
    public static class Builder
            extends RabbitMQConnectionConfig.Builder<RMQConnectionConfig.Builder> {
        public RMQConnectionConfig build() {
            return new RMQConnectionConfig(this);
        }
    }
}
