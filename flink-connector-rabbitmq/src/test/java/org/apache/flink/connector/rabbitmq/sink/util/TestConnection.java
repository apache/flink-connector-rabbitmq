package org.apache.flink.connector.rabbitmq.sink.util;

import org.apache.flink.connector.rabbitmq.common.RabbitMQConnectionConfig;

import com.rabbitmq.client.BlockedCallback;
import com.rabbitmq.client.BlockedListener;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ExceptionHandler;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.client.UnblockedCallback;
import com.rabbitmq.client.impl.AMQConnection;
import com.rabbitmq.client.impl.Frame;
import com.rabbitmq.client.impl.FrameHandler;

import java.io.IOException;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.Map;
import java.util.function.Supplier;

/** Test connection for RabbitMQ. */
public class TestConnection implements Connection {

    private final RabbitMQConnectionConfig connectionConfig;
    private boolean isOpen = true;

    private Supplier<Channel> channelSupplier;

    public TestConnection(RabbitMQConnectionConfig connectionConfig) {
        this.connectionConfig = connectionConfig;
    }

    public void setChannelSupplier(Supplier<Channel> channelSupplier) {
        this.channelSupplier = channelSupplier;
    }

    @Override
    public InetAddress getAddress() {
        return null;
    }

    @Override
    public int getPort() {
        return connectionConfig.getPort();
    }

    @Override
    public int getChannelMax() {
        return connectionConfig.getRequestedChannelMax();
    }

    @Override
    public int getFrameMax() {
        return connectionConfig.getRequestedFrameMax();
    }

    @Override
    public int getHeartbeat() {
        return 0;
    }

    @Override
    public Map<String, Object> getClientProperties() {
        return null;
    }

    @Override
    public String getClientProvidedName() {
        return null;
    }

    @Override
    public Map<String, Object> getServerProperties() {
        return null;
    }

    @Override
    public Channel createChannel() throws IOException {
        return channelSupplier.get();
    }

    @Override
    public Channel createChannel(int i) throws IOException {
        return channelSupplier.get();
    }

    @Override
    public void close() throws IOException {
        isOpen = false;
    }

    @Override
    public void close(int i, String s) throws IOException {
        isOpen = false;
    }

    @Override
    public void close(int i) throws IOException {
        isOpen = false;
    }

    @Override
    public void close(int i, String s, int i1) throws IOException {
        isOpen = false;
    }

    @Override
    public void abort() {}

    @Override
    public void abort(int i, String s) {}

    @Override
    public void abort(int i) {}

    @Override
    public void abort(int i, String s, int i1) {}

    @Override
    public void addBlockedListener(BlockedListener blockedListener) {}

    @Override
    public BlockedListener addBlockedListener(
            BlockedCallback blockedCallback, UnblockedCallback unblockedCallback) {
        return null;
    }

    @Override
    public boolean removeBlockedListener(BlockedListener blockedListener) {
        return false;
    }

    @Override
    public void clearBlockedListeners() {}

    @Override
    public ExceptionHandler getExceptionHandler() {
        return null;
    }

    @Override
    public String getId() {
        return null;
    }

    @Override
    public void setId(String s) {}

    @Override
    public void addShutdownListener(ShutdownListener shutdownListener) {}

    @Override
    public void removeShutdownListener(ShutdownListener shutdownListener) {}

    @Override
    public ShutdownSignalException getCloseReason() {
        return null;
    }

    @Override
    public void notifyListeners() {}

    @Override
    public boolean isOpen() {
        return isOpen;
    }

    /** No-op frame handler for RabbitMQ. */
    public static class TestFrameHandler implements FrameHandler {
        public TestFrameHandler() {}

        @Override
        public void setTimeout(int i) throws SocketException {}

        @Override
        public int getTimeout() throws SocketException {
            return 0;
        }

        @Override
        public void sendHeader() throws IOException {}

        @Override
        public void initialize(AMQConnection amqConnection) {}

        @Override
        public Frame readFrame() throws IOException {
            return null;
        }

        @Override
        public void writeFrame(Frame frame) throws IOException {}

        @Override
        public void flush() throws IOException {}

        @Override
        public void close() {}

        @Override
        public InetAddress getLocalAddress() {
            return null;
        }

        @Override
        public int getLocalPort() {
            return 0;
        }

        @Override
        public InetAddress getAddress() {
            return null;
        }

        @Override
        public int getPort() {
            return 0;
        }
    }
}
