/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.client.impl.consumer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.ConsumeReturnType;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.hook.ConsumeMessageContext;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.client.stat.ConsumerStatsManager;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.CMResult;
import org.apache.rocketmq.common.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.common.RemotingHelper;

public class ConsumeMessageConcurrentlyService implements ConsumeMessageService {
    private static final InternalLogger log = ClientLogger.getLog();
    private final DefaultMQPushConsumerImpl defaultMQPushConsumerImpl;
    private final DefaultMQPushConsumer defaultMQPushConsumer;
    private final MessageListenerConcurrently messageListener;
    private final BlockingQueue<Runnable> consumeRequestQueue;
    private final ThreadPoolExecutor consumeExecutor;
    private final String consumerGroup;

    private final ScheduledExecutorService scheduledExecutorService;
    private final ScheduledExecutorService cleanExpireMsgExecutors;

    public ConsumeMessageConcurrentlyService(DefaultMQPushConsumerImpl defaultMQPushConsumerImpl,
        MessageListenerConcurrently messageListener) {
        this.defaultMQPushConsumerImpl = defaultMQPushConsumerImpl;
        this.messageListener = messageListener;

        this.defaultMQPushConsumer = this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer();
        this.consumerGroup = this.defaultMQPushConsumer.getConsumerGroup();
        this.consumeRequestQueue = new LinkedBlockingQueue<Runnable>();

        String consumeThreadPrefix = null;
        if (consumerGroup.length() > 100) {
            consumeThreadPrefix = new StringBuilder("ConsumeMessageThread_").append(consumerGroup, 0, 100).append("_").toString();
        } else {
            consumeThreadPrefix = new StringBuilder("ConsumeMessageThread_").append(consumerGroup).append("_").toString();
        }
        /*
         * 并发消费线程池
         * 最小、最大线程数默认20，阻塞队列为无界阻塞队列LinkedBlockingQueue
         */
        this.consumeExecutor = new ThreadPoolExecutor(
            this.defaultMQPushConsumer.getConsumeThreadMin(),
            this.defaultMQPushConsumer.getConsumeThreadMax(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            this.consumeRequestQueue,
            new ThreadFactoryImpl(consumeThreadPrefix));

        //单线程的延迟任务线程池，用于延迟提交消费请求
        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("ConsumeMessageScheduledThread_"));

        //单线程的延迟任务线程池，用于处理过期的消息
        this.cleanExpireMsgExecutors = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("CleanExpireMsgScheduledThread_"));
    }

    /**
     * 通过cleanExpireMsgExecutors定时任务清理过期的消息，启动后15min开始执行，
     * 后每15min执行一次，这里的15min是RocketMQ大的默认超时时间，可通过defaultMQPushConsumer#consumeTimeout属性设置。
     *
     */
    public void start() {
        //通过cleanExpireMsgExecutors定时任务清理过期的消息
        //启动后15min开始执行，后每15min执行一次，这里的15min时RocketMQ大的默认超时时间，可通过defaultMQPushConsumer#consumeTimeout属性设置
        this.cleanExpireMsgExecutors.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                try {
                    //清理过期消息
                    cleanExpireMsg();
                } catch (Throwable e) {
                    log.error("scheduleAtFixedRate cleanExpireMsg exception", e);
                }
            }

        }, this.defaultMQPushConsumer.getConsumeTimeout(), this.defaultMQPushConsumer.getConsumeTimeout(), TimeUnit.MINUTES);
    }

    public void shutdown(long awaitTerminateMillis) {
        this.scheduledExecutorService.shutdown();
        ThreadUtils.shutdownGracefully(this.consumeExecutor, awaitTerminateMillis, TimeUnit.MILLISECONDS);
        this.cleanExpireMsgExecutors.shutdown();
    }

    @Override
    public void updateCorePoolSize(int corePoolSize) {
        if (corePoolSize > 0
            && corePoolSize <= Short.MAX_VALUE
            && corePoolSize < this.defaultMQPushConsumer.getConsumeThreadMax()) {
            this.consumeExecutor.setCorePoolSize(corePoolSize);
        }
    }

    @Override
    public void incCorePoolSize() {

    }

    @Override
    public void decCorePoolSize() {

    }

    @Override
    public int getCorePoolSize() {
        return this.consumeExecutor.getCorePoolSize();
    }

    @Override
    public ConsumeMessageDirectlyResult consumeMessageDirectly(MessageExt msg, String brokerName) {
        ConsumeMessageDirectlyResult result = new ConsumeMessageDirectlyResult();
        result.setOrder(false);
        result.setAutoCommit(true);

        List<MessageExt> msgs = new ArrayList<MessageExt>();
        msgs.add(msg);
        MessageQueue mq = new MessageQueue();
        mq.setBrokerName(brokerName);
        mq.setTopic(msg.getTopic());
        mq.setQueueId(msg.getQueueId());

        ConsumeConcurrentlyContext context = new ConsumeConcurrentlyContext(mq);

        this.defaultMQPushConsumerImpl.resetRetryAndNamespace(msgs, this.consumerGroup);

        final long beginTime = System.currentTimeMillis();

        log.info("consumeMessageDirectly receive new message: {}", msg);

        try {
            ConsumeConcurrentlyStatus status = this.messageListener.consumeMessage(msgs, context);
            if (status != null) {
                switch (status) {
                    case CONSUME_SUCCESS:
                        result.setConsumeResult(CMResult.CR_SUCCESS);
                        break;
                    case RECONSUME_LATER:
                        result.setConsumeResult(CMResult.CR_LATER);
                        break;
                    default:
                        break;
                }
            } else {
                result.setConsumeResult(CMResult.CR_RETURN_NULL);
            }
        } catch (Throwable e) {
            result.setConsumeResult(CMResult.CR_THROW_EXCEPTION);
            result.setRemark(RemotingHelper.exceptionSimpleDesc(e));

            log.warn(String.format("consumeMessageDirectly exception: %s Group: %s Msgs: %s MQ: %s",
                RemotingHelper.exceptionSimpleDesc(e),
                ConsumeMessageConcurrentlyService.this.consumerGroup,
                msgs,
                mq), e);
        }

        result.setSpentTimeMills(System.currentTimeMillis() - beginTime);

        log.info("consumeMessageDirectly Result: {}", result);

        return result;
    }

    /**
     * 该方法将消息批量的封装为ConsumeRequest提交到ConsumeMessageConcurrentlyService内部的consumeExecutor线程池中进行异步消费，
     * 如果提交失败，则调用submitConsumeRequestLater方法延迟5s进行提交，而不是丢弃。
     *
     * 1. 首先获取单次批量消费的数量，默认1，通过DefaultMQPushConsumer的consumeMessageBatchMaxSize属性配置。
     * 2. 如果消息数量 <= 单次批量消费的数量，那么直接全量消费，构建一个ConsumeRequest并提交到consumeExecutor线程池。
     * 3. 如果消息数量 > 单次批量消费的数量，那么需要分割消息进行分批提交。
     *
     * 从该方法可以得知，对于并发消费模式，拉取到的一批消息被分批次提交到线程池之后，就由线程池里面的线程异步的消费，
     * 我们知道线程池里面的线程执行先后顺序时不可控制的，因此这些不同批次的消息会被并发、的无序的消费。
     *
     * @param msgs              拉取到的消息
     * @param processQueue      处理队列
     * @param messageQueue      消息队列
     * @param dispatchToConsume 是否分发消费，对于并发消费无影响
     */
    @Override
    public void submitConsumeRequest(
        final List<MessageExt> msgs,
        final ProcessQueue processQueue,
        final MessageQueue messageQueue,
        final boolean dispatchToConsume) {
        //单次批量消费的数量，默认1,
        // 它代表着每次发送给消息监听器MessageListenerOrderly或者MessageListenerConcurrently的consumeMessage方法中的参数List msgs中的最多的消息数量
        final int consumeBatchSize = this.defaultMQPushConsumer.getConsumeMessageBatchMaxSize();
        /*
         * 如果消息数量 <= 单次批量消费的数量，那么直接全量消费
         */
        if (msgs.size() <= consumeBatchSize) {
            //构建消费请求，将消息全部放进去
            ConsumeRequest consumeRequest = new ConsumeRequest(msgs, processQueue, messageQueue);
            try {
                //将请求提交到consumeExecutor线程池中进行消费
                this.consumeExecutor.submit(consumeRequest);
            } catch (RejectedExecutionException e) {
                //提交的任务被线程池拒绝，那么延迟5s进行提交，而不是丢弃
                this.submitConsumeRequestLater(consumeRequest);
            }
        }
        /*
         * 如果消息数量 > 单次批量消费的数量，那么需要分割消息进行分批提交
         */
        else {
            //遍历
            for (int total = 0; total < msgs.size(); ) {
                //一批消息集合，每批消息最多consumeBatchSize条，默认1
                List<MessageExt> msgThis = new ArrayList<MessageExt>(consumeBatchSize);
                //将消息按顺序加入集合
                for (int i = 0; i < consumeBatchSize; i++, total++) {
                    if (total < msgs.size()) {
                        msgThis.add(msgs.get(total));
                    } else {
                        break;
                    }
                }

                //将本批次消息构建为ConsumeRequest
                ConsumeRequest consumeRequest = new ConsumeRequest(msgThis, processQueue, messageQueue);
                try {
                    //将请求提交到consumeExecutor线程池中进行消费
                    this.consumeExecutor.submit(consumeRequest);
                } catch (RejectedExecutionException e) {
                    for (; total < msgs.size(); total++) {
                        msgThis.add(msgs.get(total));
                    }

                    //提交的任务被线程池拒绝，那么延迟5s进行提交，而不是丢弃
                    this.submitConsumeRequestLater(consumeRequest);
                }
            }
        }
    }


    /**
     * 该方法获取所有的消息队列和处理队列的键值对，循环遍历并且调用ProcessQueue#cleanExpiredMsg方法清理过期消息。
     *
     */
    private void cleanExpireMsg() {
        //获取所有的消息队列和处理队列的键值对
        Iterator<Map.Entry<MessageQueue, ProcessQueue>> it =
            this.defaultMQPushConsumerImpl.getRebalanceImpl().getProcessQueueTable().entrySet().iterator();
        //循环遍历
        while (it.hasNext()) {
            Map.Entry<MessageQueue, ProcessQueue> next = it.next();
            ProcessQueue pq = next.getValue();
            //调用ProcessQueue#cleanExpiredMsg方法清理过期消息
            pq.cleanExpiredMsg(this.defaultMQPushConsumer);
        }
    }

    /**
     * 对于并发消费的消费结果通过ConsumeMessageConcurrentlyService#processConsumeResult方法处理。
     *
     * 需要注意的是，如果在执行了listener#consumeMessage方法，即执行了业务逻辑之后，处理消费结果之前，该消息队列被丢弃了，
     * 例如负载均衡时该队列被分配给了其他新上线的消费者，那么由于dropped=false，
     * 导致不会进行最后的消费结果处理，将会导致消息的重复消费，因此必须做好业务层面的幂等性！
     *
     * processConsumeResult方法的大概步骤为：
     *
     * 1. 获取ackIndex，默认初始值为Integer.MAX_VALUE，该值表示消费成功的消息在消息集合中的索引，用于辅助进行消息重试。
     * 2. 判断消费状态，设置ackIndex的值：
     *      2.1 CONSUME_SUCCESS消费成功： ackIndex = 消息数量 – 1。
     *      2.2 RECONSUME_LATER消费失败： ackIndex = -1。
     * 3. 判断消息模式，处理消费失败的情况：
     *      3.1 BROADCASTING广播模式：对于没有消费成功的消息仅仅打印日志。
     *      3.2 CLUSTERING集群模式：
     *          3.2.1 对于消费失败的消息，调用sendMessageBack方法向broker发送发回当前消息作为延迟消息到重试队列，等待重试消费。
     *              对于sendMessageBack发送失败的消息加入msgBackFailed失败集合，设置消息的重试次数属性reconsumeTimes+1
     *          3.2.2 对于sendMessageBack发送失败的消息，调用submitConsumeRequestLater方法，
     *              延迟5s将sendMessageBack执行失败的消息再次提交到consumeExecutor进行消费。
     * 4. 调用ProcessQueue#removeMessage方法从处理队列的msgTreeMap中将消费成功的消息，
     *      以及消费失败但是发回broker成功的这批消息移除，然后返回msgTreeMap中的最小的偏移量。
     * 5. 如果偏移量大于等于0并且处理队列没有被丢弃，调用OffsetStore# updateOffset方法，
     *      尝试更新内存中的offsetTable中的最新偏移量信息，第三个参数是否仅单调增加offset为true，表示只会尝试更新offset为更大的值。
     *      这里仅仅是更新内存中的数据，而offset除了在拉取消息时上报broker进行持久化之外，
     *      还会定时每5s调用persistAllConsumerOffset定时持久化
     *
     * @param status
     * @param context
     * @param consumeRequest
     */
    public void processConsumeResult(
        final ConsumeConcurrentlyStatus status,
        final ConsumeConcurrentlyContext context,
        final ConsumeRequest consumeRequest
    ) {
        //ackIndex，默认初始值为Integer.MAX_VALUE，表示消费成功的消息在消息集合中的索引
        int ackIndex = context.getAckIndex();

        //如果消息为空则直接返回
        if (consumeRequest.getMsgs().isEmpty())
            return;

        /*
         * 1 判断消费状态，设置ackIndex的值
         * 消费成功： ackIndex = 消息数量 - 1
         * 消费失败： ackIndex = -1
         */
        switch (status) {
            //如果消费成功
            case CONSUME_SUCCESS:
                //如果大于等于消息数量，则设置为消息数量减1
                //初始值为Integer.MAX_VALUE，因此一般都会设置为消息数量减1
                if (ackIndex >= consumeRequest.getMsgs().size()) {
                    ackIndex = consumeRequest.getMsgs().size() - 1;
                }
                //消费成功的个数，即消息数量
                int ok = ackIndex + 1;
                //消费失败的个数，即0
                int failed = consumeRequest.getMsgs().size() - ok;
                //统计
                this.getConsumerStatsManager().incConsumeOKTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), ok);
                this.getConsumerStatsManager().incConsumeFailedTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), failed);
                break;
            //如果消费失败
            case RECONSUME_LATER:
                //ackIndex初始化为-1
                ackIndex = -1;
                //统计
                this.getConsumerStatsManager().incConsumeFailedTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(),
                    consumeRequest.getMsgs().size());
                break;
            default:
                break;
        }

        /*
         * 2 判断消息模式，处理消费失败的情况
         * 广播模式：打印日志
         * 集群模式：向broker发送当前消息作为延迟消息，等待重试消费
         */
        switch (this.defaultMQPushConsumer.getMessageModel()) {
            //广播模式下
            case BROADCASTING:
                for (int i = ackIndex + 1; i < consumeRequest.getMsgs().size(); i++) {
                    //从消费成功的消息在消息集合中的索引+1开始，仅仅是对于消费失败的消息打印日志，并不会重试
                    MessageExt msg = consumeRequest.getMsgs().get(i);
                    log.warn("BROADCASTING, the message consume failed, drop it, {}", msg.toString());
                }
                break;
            //集群模式下
            case CLUSTERING:
                List<MessageExt> msgBackFailed = new ArrayList<MessageExt>(consumeRequest.getMsgs().size());
                //消费成功的消息在消息集合中的索引+1开始，遍历消息
                for (int i = ackIndex + 1; i < consumeRequest.getMsgs().size(); i++) {
                    // 如果ackIndex为-1，则表示消费失败，会进入该循环
                    // 如果ackIndex不为-1，则是消息数量-1，此时+1则会等于消息数量，不会进入循环
                    //获取该索引对应的消息
                    MessageExt msg = consumeRequest.getMsgs().get(i);
                    /*
                     * 2.1 消费失败后，将该消息重新发送至重试队列，延迟消费
                     */
                    boolean result = this.sendMessageBack(msg, context);
                    //如果执行发送失败
                    if (!result) {
                        //设置重试次数+1
                        msg.setReconsumeTimes(msg.getReconsumeTimes() + 1);
                        //加入失败的集合
                        msgBackFailed.add(msg);
                    }
                }

                if (!msgBackFailed.isEmpty()) {
                    //从consumeRequest中移除消费失败并且发回broker失败的消息
                    consumeRequest.getMsgs().removeAll(msgBackFailed);

                    /*
                     * 2.2 调用submitConsumeRequestLater方法，延迟5s将sendMessageBack执行失败的消息再次提交到consumeExecutor进行消费
                     */
                    this.submitConsumeRequestLater(msgBackFailed, consumeRequest.getProcessQueue(), consumeRequest.getMessageQueue());
                }
                break;
            default:
                break;
        }

        /*
         * 3 从处理队列的msgTreeMap中将消费成功以及消费失败但是发回broker成功的这批消息移除，然后返回msgTreeMap中的最小的偏移量
         */
        long offset = consumeRequest.getProcessQueue().removeMessage(consumeRequest.getMsgs());
        //如果偏移量大于等于0并且处理队列没有被丢弃
        if (offset >= 0 && !consumeRequest.getProcessQueue().isDropped()) {
            //尝试更新内存中的offsetTable中的最新偏移量信息，第三个参数是否仅单调增加offset为true
            this.defaultMQPushConsumerImpl.getOffsetStore().updateOffset(consumeRequest.getMessageQueue(), offset, true);
        }
    }

    public ConsumerStatsManager getConsumerStatsManager() {
        return this.defaultMQPushConsumerImpl.getConsumerStatsManager();
    }

    /**
     * ConsumeMessageConcurrentlyService的方法，并发消费失败，发送消费失败的消息到broker。
     * 从ConsumeConcurrentlyContext获取delayLevelWhenNextConsume属性作为延迟等级，默认为0。通过在业务方法中修改该属性的只可以控制延迟等级：
     *
     * 1. -1，不重试，直接发往死信队列。
     * 2. 0，默认值，延迟等级broker端控制，默认从延迟等级level3开始，后续每次重试都是3 + 当前重试次数。
     * 3. 大于0，由client端控制，传入多少延迟等级就是多少。
     * 注意，每次消费均产生一个新的ConsumeConcurrentlyContext对象，所以仅能设置单次发回消息的延迟等级

     *
     * @param msg
     * @param context
     * @return
     */
    public boolean sendMessageBack(final MessageExt msg, final ConsumeConcurrentlyContext context) {
        /*
         * 从ConsumeConcurrentlyContext获取delayLevelWhenNextConsume属性作为延迟等级，默认为0。
         * 通过在业务方法中修改该属性的只可以控制延迟等级：
         * -1，不重试，直接发往死信队列。
         * 0，默认值，延迟等级broker端控制的，默认从延迟等级level3开始，后续每次重试都是3 + 当前重试次数。
         * 大于0，由client端控制，传入多少延迟等级就是多少。
         *
         * 注意，每次消费均产生一个新的ConsumeConcurrentlyContext对象，所以仅能设置单次发回消息的延迟等级
         */
        int delayLevel = context.getDelayLevelWhenNextConsume();

        // Wrap topic with namespace before sending back message.
        //使用nameSpace包装topic为重试topic
        msg.setTopic(this.defaultMQPushConsumer.withNamespace(msg.getTopic()));
        try {
            //调用DefaultMQPushConsumerImpl#sendMessageBack方法发送消费失败的消息到broker
            this.defaultMQPushConsumerImpl.sendMessageBack(msg, delayLevel, context.getMessageQueue().getBrokerName());
            //没有抛出异常就算成功
            return true;
        } catch (Exception e) {
            log.error("sendMessageBack exception, group: " + this.consumerGroup + " msg: " + msg, e);
        }

        return false;
    }

    private void submitConsumeRequestLater(
        final List<MessageExt> msgs,
        final ProcessQueue processQueue,
        final MessageQueue messageQueue
    ) {

        this.scheduledExecutorService.schedule(new Runnable() {

            @Override
            public void run() {
                ConsumeMessageConcurrentlyService.this.submitConsumeRequest(msgs, processQueue, messageQueue, true);
            }
        }, 5000, TimeUnit.MILLISECONDS);
    }

    /**
     * ConsumeMessageConcurrentlyService的方法
     *
     * 提交的任务被线程池拒绝，那么延迟5s进行提交，而不是丢弃
     * @param consumeRequest 提交请求
     */
    private void submitConsumeRequestLater(final ConsumeRequest consumeRequest
    ) {

        this.scheduledExecutorService.schedule(new Runnable() {

            @Override
            public void run() {
                //将提交的行为封装为一个线程任务，提交到scheduledExecutorService延迟线程池，5s之后执
                ConsumeMessageConcurrentlyService.this.consumeExecutor.submit(consumeRequest);
            }
        }, 5000, TimeUnit.MILLISECONDS);
    }

    /**
     * ConsumeRequest本身是一个线程任务，当拉取到消息之后，会将一批消息构建为一个ConsumeRequest对象，提交给consumeExecutor，
     * 由线程池异步的执行，它的run方法就是并发消费的核心方法。大概逻辑为：
     *
     * 1. 如果处理队列被丢弃，即dropped=true，那么直接返回，不再消费，例如负载均衡时该队列被分配给了其他新上线的消费者，尽量避免重复消费。
     * 2. 调用resetRetryAndNamespace方法，当消息是重试消息的时候，将msg的topic属性从重试topic还原为真实的topic。
     * 3. 如果有消费钩子，那么执行钩子函数的前置方法consumeMessageBefore。
     *      我们可以通过DefaultMQPushConsumerImpl#registerConsumeMessageHook方法注册消费钩子ConsumeMessageHook，在消费消息的前后调用。
     * 4. 调用listener#consumeMessage方法，进行消息消费，调用实际的业务逻辑，返回执行状态结果如status为null。
     *      4.1 正常情况下可返回两种状态：ConsumeConcurrentlyStatus.CONSUME_SUCCESS表示消费成功 ，
     *          ConsumeConcurrentlyStatus.RECONSUME_LATER表示消费失败，如果中途抛出异常，则status为null。
     * 5. 对返回的执行状态结果进行判断处理。从这里可得知消费超时时间为15min，另外如果返回的status为null，那么status将会被设置为RECONSUME_LATER，即消费失败。
     *      5.1 计算消费时间consumeRT。如果status为null，如果业务的执行抛出了异常，设置returnType为EXCEPTION，否则设置returnType为RETURNNULL。
     *      5.2 如消费时间consumeRT大于等于consumeTimeout，默认15min。设置returnType为TIME_OUT。
     *          消费超时时间可通过DefaultMQPushConsumer. consumeTimeout属性配置，默认15，单位分钟。
     *      5.3 如status为RECONSUME_LATER，即消费失败，设置returnType为FAILED。
     *      5.4 如status为CONSUME_SUCCESS，即消费成功，设置returnType为SUCCESS。
     * 6. 如果有消费钩子，那么执行钩子函数的后置方法consumeMessageAfter。
     * 7. 如果处理队列没有被丢弃，即dropped=false，那么调用ConsumeMessageConcurrentlyService#processConsumeResult方法处理消费结果，包含消费重试、提交offset等操作。
     *
     * 需要注意的是，如果在执行了listener#consumeMessage方法，即执行了业务逻辑之后，处理消费结果之前，该消息队列被丢弃了，
     * 例如负载均衡时该队列被分配给了其他新上线的消费者，那么由于dropped=false，导致不会进行最后的消费结果处理，将会导致消息的重复消费，因此必须做好业务层面的幂等性！
     *
     */
    class ConsumeRequest implements Runnable {
        //一次消费的消息集合，默认1条消息
        private final List<MessageExt> msgs;
        //处理队列
        private final ProcessQueue processQueue;
        //消息队列
        private final MessageQueue messageQueue;

        public ConsumeRequest(List<MessageExt> msgs, ProcessQueue processQueue, MessageQueue messageQueue) {
            this.msgs = msgs;
            this.processQueue = processQueue;
            this.messageQueue = messageQueue;
        }

        public List<MessageExt> getMsgs() {
            return msgs;
        }

        public ProcessQueue getProcessQueue() {
            return processQueue;
        }

        @Override
        public void run() {
            //如果处理队列被丢弃，那么直接返回，不再消费，例如负载均衡时该队列被分配给了其他新上线的消费者，尽量避免重复消费
            if (this.processQueue.isDropped()) {
                log.info("the message queue not be able to consume, because it's dropped. group={} {}", ConsumeMessageConcurrentlyService.this.consumerGroup, this.messageQueue);
                return;
            }

            /*
             * 1 获取并发消费的消息监听器，push模式模式下是我们需要开发的，通过registerMessageListener方法注册，内部包含了要执行的业务逻辑
             */
            MessageListenerConcurrently listener = ConsumeMessageConcurrentlyService.this.messageListener;
            ConsumeConcurrentlyContext context = new ConsumeConcurrentlyContext(messageQueue);
            ConsumeConcurrentlyStatus status = null;
            //重置重试topic
            defaultMQPushConsumerImpl.resetRetryAndNamespace(msgs, defaultMQPushConsumer.getConsumerGroup());

            /*
             * 2 如果有消费钩子，那么执行钩子函数的前置方法consumeMessageBefore
             * 我们可以注册钩子ConsumeMessageHook，再消费消息的前后调用
             */
            ConsumeMessageContext consumeMessageContext = null;
            if (ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                consumeMessageContext = new ConsumeMessageContext();
                consumeMessageContext.setNamespace(defaultMQPushConsumer.getNamespace());
                consumeMessageContext.setConsumerGroup(defaultMQPushConsumer.getConsumerGroup());
                consumeMessageContext.setProps(new HashMap<String, String>());
                consumeMessageContext.setMq(messageQueue);
                consumeMessageContext.setMsgList(msgs);
                consumeMessageContext.setSuccess(false);
                ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.executeHookBefore(consumeMessageContext);
            }

            //起始时间戳
            long beginTimestamp = System.currentTimeMillis();
            boolean hasException = false;
            //消费返回类型，初始化为SUCCESS
            ConsumeReturnType returnType = ConsumeReturnType.SUCCESS;
            try {
                if (msgs != null && !msgs.isEmpty()) {
                    //循环设置每个消息的起始消费时间
                    for (MessageExt msg : msgs) {
                        MessageAccessor.setConsumeStartTimeStamp(msg, String.valueOf(System.currentTimeMillis()));
                    }
                }
                /*
                 * 3 调用listener#consumeMessage方法，进行消息消费，调用实际的业务逻辑，返回执行状态结果
                 * 有两种状态ConsumeConcurrentlyStatus.CONSUME_SUCCESS 和 ConsumeConcurrentlyStatus.RECONSUME_LATER
                 */
                status = listener.consumeMessage(Collections.unmodifiableList(msgs), context);
            } catch (Throwable e) {
                log.warn(String.format("consumeMessage exception: %s Group: %s Msgs: %s MQ: %s",
                    RemotingHelper.exceptionSimpleDesc(e),
                    ConsumeMessageConcurrentlyService.this.consumerGroup,
                    msgs,
                    messageQueue), e);
                //抛出异常之后，设置异常标志位
                hasException = true;
            }
            /*
             * 4 对返回的执行状态结果进行判断处理
             */
            //计算消费时间
            long consumeRT = System.currentTimeMillis() - beginTimestamp;
            //如status为null
            if (null == status) {
                //如果业务的执行抛出了异常
                if (hasException) {
                    //设置returnType为EXCEPTION
                    returnType = ConsumeReturnType.EXCEPTION;
                } else {
                    //设置returnType为RETURNNULL
                    returnType = ConsumeReturnType.RETURNNULL;
                }
            }
            //如消费时间consumeRT大于等于consumeTimeout，默认15min
            else if (consumeRT >= defaultMQPushConsumer.getConsumeTimeout() * 60 * 1000) {
                //设置returnType为TIME_OUT
                returnType = ConsumeReturnType.TIME_OUT;
            }
            //如status为RECONSUME_LATER，即消费失败
            else if (ConsumeConcurrentlyStatus.RECONSUME_LATER == status) {
                //设置returnType为FAILED
                returnType = ConsumeReturnType.FAILED;
            }
            //如status为CONSUME_SUCCESS，即消费成功
            else if (ConsumeConcurrentlyStatus.CONSUME_SUCCESS == status) {
                //设置returnType为SUCCESS，即消费成功
                returnType = ConsumeReturnType.SUCCESS;
            }

            //如果有钩子，则将returnType设置进去
            if (ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                consumeMessageContext.getProps().put(MixAll.CONSUME_CONTEXT_TYPE, returnType.name());
            }

            //如果status为null
            if (null == status) {
                log.warn("consumeMessage return null, Group: {} Msgs: {} MQ: {}",
                    ConsumeMessageConcurrentlyService.this.consumerGroup,
                    msgs,
                    messageQueue);
                //将status设置为RECONSUME_LATER，即消费失败
                status = ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }

            /*
             * 5 如果有消费钩子，那么执行钩子函数的后置方法consumeMessageAfter
             * 我们可以注册钩子ConsumeMessageHook，在消费消息的前后调用
             */
            if (ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                consumeMessageContext.setStatus(status.toString());
                consumeMessageContext.setSuccess(ConsumeConcurrentlyStatus.CONSUME_SUCCESS == status);
                consumeMessageContext.setAccessChannel(defaultMQPushConsumer.getAccessChannel());
                ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.executeHookAfter(consumeMessageContext);
            }

            //增加消费时间
            ConsumeMessageConcurrentlyService.this.getConsumerStatsManager()
                .incConsumeRT(ConsumeMessageConcurrentlyService.this.consumerGroup, messageQueue.getTopic(), consumeRT);

            /*
             * 6 如果处理队列没有被丢弃，那么调用ConsumeMessageConcurrentlyService#processConsumeResult方法处理消费结果，包含重试等逻辑
             */
            if (!processQueue.isDropped()) {
                ConsumeMessageConcurrentlyService.this.processConsumeResult(status, context, this);
            } else {
                log.warn("processQueue is dropped without process consume result. messageQueue={}, msgs={}", messageQueue, msgs);
            }
        }

        public MessageQueue getMessageQueue() {
            return messageQueue;
        }

    }
}
