/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.rabbitmq.sink.state;

import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.connector.rabbitmq.sink.common.RabbitMQSinkMessageWrapper;
import org.apache.flink.connector.rabbitmq.sink.writer.specialized.RabbitMQSinkWriterAtLeastOnce;
import org.apache.flink.connector.rabbitmq.sink.writer.specialized.RabbitMQSinkWriterExactlyOnce;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * The state of a {@link SinkWriter} implementation. Contains {@code outstandingMessages} that could
 * not be delivered in a checkpoint. Used in the {@link RabbitMQSinkWriterAtLeastOnce} and {@link
 * RabbitMQSinkWriterExactlyOnce} implementations.
 */
public class RabbitMQSinkWriterState<T> {
    private final List<RabbitMQSinkMessageWrapper<T>> outstandingMessages;

    public RabbitMQSinkWriterState(List<RabbitMQSinkMessageWrapper<T>> outstandingMessages) {
        this.outstandingMessages = requireNonNull(outstandingMessages);
    }

    public List<RabbitMQSinkMessageWrapper<T>> getOutstandingMessages() {
        return outstandingMessages;
    }
}
