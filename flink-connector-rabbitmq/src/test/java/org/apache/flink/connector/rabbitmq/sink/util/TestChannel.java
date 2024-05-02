package org.apache.flink.connector.rabbitmq.sink.util;

import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.connector.rabbitmq.common.SerializableReturnListener;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Command;
import com.rabbitmq.client.ConfirmCallback;
import com.rabbitmq.client.ConfirmListener;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.ConsumerShutdownSignalCallback;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.Method;
import com.rabbitmq.client.ReturnCallback;
import com.rabbitmq.client.ReturnListener;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.client.impl.AMQImpl;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;

/**
 * TestChannel is a test implementation of the Channel class that allows for testing the
 * RabbitMQSink.
 */
public class TestChannel implements Channel {

    private int seqNo = 0;
    private boolean isOpen = true;

    private String queueName;

    private boolean isQueueDeclared = false;

    private boolean isQueueDurable = false;

    private boolean isQueueExclusive = false;

    private boolean isQueueAutoDelete = false;

    private Map<String, Object> queueArguments;

    private ConfirmCallback onSuccess;
    private ConfirmCallback onFail;

    private SerializableReturnListener returnListener;

    private final Predicate<byte[]> shouldFail;
    private final Set<String> alreadyFailed = new HashSet<>();
    private final Map<String, Long> seqNoMap = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<String, CountDownLatch> latchMap = new ConcurrentHashMap<>();

    private final Map<String, Thread> messageThreads = new ConcurrentHashMap<>();
    private final BlockingQueue<String> deliveredMessages = new ArrayBlockingQueue<>(1000);

    private final Map<String, Tuple5<String, String, Boolean, Boolean, AMQP.BasicProperties>>
            publishedMessageArguments = new HashMap<>();

    public TestChannel(Predicate<byte[]> shouldFail) {
        this.shouldFail = shouldFail;
    }

    @Override
    public void basicPublish(
            String exchange, String routingKey, AMQP.BasicProperties props, byte[] body)
            throws IOException {
        this.basicPublish(exchange, routingKey, false, props, body);
    }

    @Override
    public void basicPublish(
            String exchange,
            String routingKey,
            boolean mandatory,
            AMQP.BasicProperties props,
            byte[] body)
            throws IOException {
        this.basicPublish(exchange, routingKey, mandatory, false, props, body);
    }

    @Override
    public void basicPublish(
            String exchange,
            String routingKey,
            boolean mandatory,
            boolean immediate,
            AMQP.BasicProperties props,
            byte[] body)
            throws IOException {
        Thread s =
                new Thread(
                        () -> {
                            try {
                                latchMap.get(new String(body)).await();
                            } catch (InterruptedException ignored) {
                                ignored.printStackTrace();
                            }
                            if (returnListener != null) {
                                try {
                                    returnListener.handleReturn(
                                            1, "200", "replyText", exchange, null, body);
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                            }
                            if (shouldFail.test(body)
                                    && !alreadyFailed.contains(new String(body))) {
                                try {
                                    alreadyFailed.add(new String(body));
                                    onFail.handle(seqNoMap.get(new String(body)), false);
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                            } else {
                                try {
                                    deliveredMessages.add(new String(body));
                                    onSuccess.handle(seqNoMap.get(new String(body)), true);
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                            }
                        });

        publishedMessageArguments.put(
                new String(body), Tuple5.of(exchange, routingKey, mandatory, immediate, props));
        seqNoMap.put(new String(body), (long) seqNo++);
        messageThreads.put(new String(body), s);
        s.start();
    }

    public BlockingQueue<String> getDeliveredMessages() {
        return deliveredMessages;
    }

    public Tuple5<String, String, Boolean, Boolean, AMQP.BasicProperties>
            getPublishedMessageArguments(String key) {
        return publishedMessageArguments.get(key);
    }

    public void prepareMessage(String key) {
        latchMap.put(key, new CountDownLatch(1));
    }

    public void deliverMessage(String key) {
        deliverMessage(key, false);
    }

    public void deliverMessage(String key, boolean await) {
        CountDownLatch latch = latchMap.get(key);
        if (latch != null) {
            latch.countDown();
        }

        Thread t = messageThreads.get(key);
        if (t != null && await) {
            try {
                t.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public ConfirmListener addConfirmListener(ConfirmCallback onSuccess, ConfirmCallback onFail) {
        this.onSuccess = onSuccess;
        this.onFail = onFail;
        return null;
    }

    @Override
    public AMQImpl.Queue.DeclareOk queueDeclare(
            String queue,
            boolean durable,
            boolean exclusive,
            boolean autoDelete,
            Map<String, Object> arguments)
            throws IOException {
        this.queueName = queue;
        this.isQueueDurable = durable;
        this.isQueueExclusive = exclusive;
        this.isQueueAutoDelete = autoDelete;
        this.queueArguments = arguments;
        this.isQueueDeclared = true;
        return null;
    }

    public boolean isOpen() {
        return isOpen;
    }

    public boolean isQueueDurable() {
        return isQueueDurable;
    }

    public boolean isQueueExclusive() {
        return isQueueExclusive;
    }

    public boolean isQueueAutoDelete() {
        return isQueueAutoDelete;
    }

    public Map<String, Object> getQueueArguments() {
        return queueArguments;
    }

    public String getQueueName() {
        return queueName;
    }

    public boolean isQueueDeclared() {
        return isQueueDeclared;
    }

    public void deliverAllMessages() {
        deliverAllMessages(false);
    }

    public void deliverAllMessages(boolean await) {
        for (String message : messageThreads.keySet()) {
            latchMap.get(message).countDown();
        }
        if (await) {
            for (Thread t : messageThreads.values()) {
                try {
                    t.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    @Override
    public void close() throws IOException {
        deliverAllMessages();
        for (Thread t : messageThreads.values()) {
            try {
                t.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        isOpen = false;
    }

    public ReturnListener getReturnListener() {
        return returnListener;
    }

    public ConfirmCallback getOnSuccess() {
        return onSuccess;
    }

    public ConfirmCallback getOnFail() {
        return onFail;
    }

    /// The following methods are not implemented and are not needed for testing

    @Override
    public AMQP.Exchange.DeclareOk exchangeDeclare(String s, String s1) throws IOException {
        return null;
    }

    @Override
    public AMQP.Exchange.DeclareOk exchangeDeclare(
            String s, BuiltinExchangeType builtinExchangeType) throws IOException {
        return null;
    }

    @Override
    public AMQP.Exchange.DeclareOk exchangeDeclare(String s, String s1, boolean b)
            throws IOException {
        return null;
    }

    @Override
    public AMQP.Exchange.DeclareOk exchangeDeclare(
            String s, BuiltinExchangeType builtinExchangeType, boolean b) throws IOException {
        return null;
    }

    @Override
    public AMQP.Exchange.DeclareOk exchangeDeclare(
            String s, String s1, boolean b, boolean b1, Map<String, Object> map)
            throws IOException {
        return null;
    }

    @Override
    public AMQP.Exchange.DeclareOk exchangeDeclare(
            String s,
            BuiltinExchangeType builtinExchangeType,
            boolean b,
            boolean b1,
            Map<String, Object> map)
            throws IOException {
        return null;
    }

    @Override
    public AMQP.Exchange.DeclareOk exchangeDeclare(
            String s, String s1, boolean b, boolean b1, boolean b2, Map<String, Object> map)
            throws IOException {
        return null;
    }

    @Override
    public AMQP.Exchange.DeclareOk exchangeDeclare(
            String s,
            BuiltinExchangeType builtinExchangeType,
            boolean b,
            boolean b1,
            boolean b2,
            Map<String, Object> map)
            throws IOException {
        return null;
    }

    @Override
    public void exchangeDeclareNoWait(
            String s, String s1, boolean b, boolean b1, boolean b2, Map<String, Object> map)
            throws IOException {}

    @Override
    public void exchangeDeclareNoWait(
            String s,
            BuiltinExchangeType builtinExchangeType,
            boolean b,
            boolean b1,
            boolean b2,
            Map<String, Object> map)
            throws IOException {}

    @Override
    public AMQP.Exchange.DeclareOk exchangeDeclarePassive(String s) throws IOException {
        return null;
    }

    @Override
    public AMQP.Exchange.DeleteOk exchangeDelete(String s, boolean b) throws IOException {
        return null;
    }

    @Override
    public void exchangeDeleteNoWait(String s, boolean b) throws IOException {}

    @Override
    public AMQP.Exchange.DeleteOk exchangeDelete(String s) throws IOException {
        return null;
    }

    @Override
    public AMQP.Exchange.BindOk exchangeBind(String s, String s1, String s2) throws IOException {
        return null;
    }

    @Override
    public AMQP.Exchange.BindOk exchangeBind(
            String s, String s1, String s2, Map<String, Object> map) throws IOException {
        return null;
    }

    @Override
    public void exchangeBindNoWait(String s, String s1, String s2, Map<String, Object> map)
            throws IOException {}

    @Override
    public AMQP.Exchange.UnbindOk exchangeUnbind(String s, String s1, String s2)
            throws IOException {
        return null;
    }

    @Override
    public AMQP.Exchange.UnbindOk exchangeUnbind(
            String s, String s1, String s2, Map<String, Object> map) throws IOException {
        return null;
    }

    @Override
    public void exchangeUnbindNoWait(String s, String s1, String s2, Map<String, Object> map)
            throws IOException {}

    @Override
    public AMQP.Queue.DeclareOk queueDeclare() throws IOException {
        return null;
    }

    @Override
    public AMQImpl.Confirm.SelectOk confirmSelect() throws IOException {
        return null;
    }

    @Override
    public void addReturnListener(ReturnListener listener) {
        this.returnListener = (SerializableReturnListener) listener;
    }

    @Override
    public ReturnListener addReturnListener(ReturnCallback returnCallback) {
        return null;
    }

    @Override
    public boolean removeReturnListener(ReturnListener returnListener) {
        return false;
    }

    @Override
    public void clearReturnListeners() {}

    @Override
    public void addConfirmListener(ConfirmListener confirmListener) {}

    @Override
    public boolean removeConfirmListener(ConfirmListener confirmListener) {
        return false;
    }

    @Override
    public void clearConfirmListeners() {}

    @Override
    public Consumer getDefaultConsumer() {
        return null;
    }

    @Override
    public void setDefaultConsumer(Consumer consumer) {}

    @Override
    public void basicQos(int i, int i1, boolean b) throws IOException {}

    @Override
    public void basicQos(int i, boolean b) throws IOException {}

    @Override
    public void basicQos(int i) throws IOException {}

    @Override
    public void queueDeclareNoWait(
            String s, boolean b, boolean b1, boolean b2, Map<String, Object> map)
            throws IOException {}

    @Override
    public AMQP.Queue.DeclareOk queueDeclarePassive(String s) throws IOException {
        return null;
    }

    @Override
    public AMQP.Queue.DeleteOk queueDelete(String s) throws IOException {
        return null;
    }

    @Override
    public AMQP.Queue.DeleteOk queueDelete(String s, boolean b, boolean b1) throws IOException {
        return null;
    }

    @Override
    public void queueDeleteNoWait(String s, boolean b, boolean b1) throws IOException {}

    @Override
    public AMQP.Queue.BindOk queueBind(String s, String s1, String s2) throws IOException {
        return null;
    }

    @Override
    public AMQP.Queue.BindOk queueBind(String s, String s1, String s2, Map<String, Object> map)
            throws IOException {
        return null;
    }

    @Override
    public void queueBindNoWait(String s, String s1, String s2, Map<String, Object> map)
            throws IOException {}

    @Override
    public AMQP.Queue.UnbindOk queueUnbind(String s, String s1, String s2) throws IOException {
        return null;
    }

    @Override
    public AMQP.Queue.UnbindOk queueUnbind(String s, String s1, String s2, Map<String, Object> map)
            throws IOException {
        return null;
    }

    @Override
    public AMQP.Queue.PurgeOk queuePurge(String s) throws IOException {
        return null;
    }

    @Override
    public GetResponse basicGet(String s, boolean b) throws IOException {
        return null;
    }

    @Override
    public void basicAck(long l, boolean b) throws IOException {}

    @Override
    public void basicNack(long l, boolean b, boolean b1) throws IOException {}

    @Override
    public void basicReject(long l, boolean b) throws IOException {}

    @Override
    public String basicConsume(String s, Consumer consumer) throws IOException {
        return null;
    }

    @Override
    public String basicConsume(
            String s, DeliverCallback deliverCallback, CancelCallback cancelCallback)
            throws IOException {
        return null;
    }

    @Override
    public String basicConsume(
            String s,
            DeliverCallback deliverCallback,
            ConsumerShutdownSignalCallback consumerShutdownSignalCallback)
            throws IOException {
        return null;
    }

    @Override
    public String basicConsume(
            String s,
            DeliverCallback deliverCallback,
            CancelCallback cancelCallback,
            ConsumerShutdownSignalCallback consumerShutdownSignalCallback)
            throws IOException {
        return null;
    }

    @Override
    public String basicConsume(String s, boolean b, Consumer consumer) throws IOException {
        return null;
    }

    @Override
    public String basicConsume(
            String s, boolean b, DeliverCallback deliverCallback, CancelCallback cancelCallback)
            throws IOException {
        return null;
    }

    @Override
    public String basicConsume(
            String s,
            boolean b,
            DeliverCallback deliverCallback,
            ConsumerShutdownSignalCallback consumerShutdownSignalCallback)
            throws IOException {
        return null;
    }

    @Override
    public String basicConsume(
            String s,
            boolean b,
            DeliverCallback deliverCallback,
            CancelCallback cancelCallback,
            ConsumerShutdownSignalCallback consumerShutdownSignalCallback)
            throws IOException {
        return null;
    }

    @Override
    public String basicConsume(String s, boolean b, Map<String, Object> map, Consumer consumer)
            throws IOException {
        return null;
    }

    @Override
    public String basicConsume(
            String s,
            boolean b,
            Map<String, Object> map,
            DeliverCallback deliverCallback,
            CancelCallback cancelCallback)
            throws IOException {
        return null;
    }

    @Override
    public String basicConsume(
            String s,
            boolean b,
            Map<String, Object> map,
            DeliverCallback deliverCallback,
            ConsumerShutdownSignalCallback consumerShutdownSignalCallback)
            throws IOException {
        return null;
    }

    @Override
    public String basicConsume(
            String s,
            boolean b,
            Map<String, Object> map,
            DeliverCallback deliverCallback,
            CancelCallback cancelCallback,
            ConsumerShutdownSignalCallback consumerShutdownSignalCallback)
            throws IOException {
        return null;
    }

    @Override
    public String basicConsume(String s, boolean b, String s1, Consumer consumer)
            throws IOException {
        return null;
    }

    @Override
    public String basicConsume(
            String s,
            boolean b,
            String s1,
            DeliverCallback deliverCallback,
            CancelCallback cancelCallback)
            throws IOException {
        return null;
    }

    @Override
    public String basicConsume(
            String s,
            boolean b,
            String s1,
            DeliverCallback deliverCallback,
            ConsumerShutdownSignalCallback consumerShutdownSignalCallback)
            throws IOException {
        return null;
    }

    @Override
    public String basicConsume(
            String s,
            boolean b,
            String s1,
            DeliverCallback deliverCallback,
            CancelCallback cancelCallback,
            ConsumerShutdownSignalCallback consumerShutdownSignalCallback)
            throws IOException {
        return null;
    }

    @Override
    public String basicConsume(
            String s,
            boolean b,
            String s1,
            boolean b1,
            boolean b2,
            Map<String, Object> map,
            Consumer consumer)
            throws IOException {
        return null;
    }

    @Override
    public String basicConsume(
            String s,
            boolean b,
            String s1,
            boolean b1,
            boolean b2,
            Map<String, Object> map,
            DeliverCallback deliverCallback,
            CancelCallback cancelCallback)
            throws IOException {
        return null;
    }

    @Override
    public String basicConsume(
            String s,
            boolean b,
            String s1,
            boolean b1,
            boolean b2,
            Map<String, Object> map,
            DeliverCallback deliverCallback,
            ConsumerShutdownSignalCallback consumerShutdownSignalCallback)
            throws IOException {
        return null;
    }

    @Override
    public String basicConsume(
            String s,
            boolean b,
            String s1,
            boolean b1,
            boolean b2,
            Map<String, Object> map,
            DeliverCallback deliverCallback,
            CancelCallback cancelCallback,
            ConsumerShutdownSignalCallback consumerShutdownSignalCallback)
            throws IOException {
        return null;
    }

    @Override
    public void basicCancel(String s) throws IOException {}

    @Override
    public AMQP.Basic.RecoverOk basicRecover() throws IOException {
        return null;
    }

    @Override
    public AMQP.Basic.RecoverOk basicRecover(boolean b) throws IOException {
        return null;
    }

    @Override
    public AMQP.Tx.SelectOk txSelect() throws IOException {
        return null;
    }

    @Override
    public AMQP.Tx.CommitOk txCommit() throws IOException {
        return null;
    }

    @Override
    public AMQP.Tx.RollbackOk txRollback() throws IOException {
        return null;
    }

    @Override
    public int getChannelNumber() {
        return 0;
    }

    @Override
    public Connection getConnection() {
        return null;
    }

    @Override
    public void close(int i, String s) throws IOException, TimeoutException {}

    @Override
    public void abort() throws IOException {}

    @Override
    public void abort(int i, String s) throws IOException {}

    @Override
    public long getNextPublishSeqNo() {
        return seqNo;
    }

    @Override
    public boolean waitForConfirms() throws InterruptedException {
        return false;
    }

    @Override
    public boolean waitForConfirms(long l) throws InterruptedException, TimeoutException {
        return false;
    }

    @Override
    public void waitForConfirmsOrDie() throws IOException, InterruptedException {}

    @Override
    public void waitForConfirmsOrDie(long l)
            throws IOException, InterruptedException, TimeoutException {}

    @Override
    public void asyncRpc(Method method) throws IOException {}

    @Override
    public Command rpc(Method method) throws IOException {
        return null;
    }

    @Override
    public long messageCount(String s) throws IOException {
        return 0;
    }

    @Override
    public long consumerCount(String s) throws IOException {
        return 0;
    }

    @Override
    public CompletableFuture<Command> asyncCompletableRpc(Method method) throws IOException {
        return null;
    }

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
}
