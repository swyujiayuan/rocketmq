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
package org.apache.rocketmq.broker.processor;

import java.net.SocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;

import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.mqtrace.ConsumeMessageContext;
import org.apache.rocketmq.broker.mqtrace.ConsumeMessageHook;
import org.apache.rocketmq.broker.mqtrace.SendMessageContext;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.common.protocol.NamespaceUtil;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.ConsumerSendMsgBackRequestHeader;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.SendMessageResponseHeader;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.sysflag.TopicSysFlag;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.RemotingResponseCallback;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.config.StorePathConfigHelper;
import org.apache.rocketmq.store.stats.BrokerStatsManager;

public class SendMessageProcessor extends AbstractSendMessageProcessor {

    private List<ConsumeMessageHook> consumeMessageHookList;

    public SendMessageProcessor(final BrokerController brokerController) {
        super(brokerController);
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx,
                                          RemotingCommand request) throws RemotingCommandException {
        RemotingCommand response = null;
        try {
            response = asyncProcessRequest(ctx, request).get();
        } catch (InterruptedException | ExecutionException e) {
            log.error("process SendMessage error, request : " + request.toString(), e);
        }
        return response;
    }

    /**
     * SendMessageProcessor的方法
     * <p>
     * 异步处理请求，默认走该方法
     *
     * 生产者发送消息的请求，将会被Broker的SendMessageProcessor处理器处理，并且被SendMessageProcessor执行器并发执行。
     *
     * SendMessageProcessor属于AsyncNettyRequestProcessor，因此会调用asyncProcessRequest方法执行请求和响应回调函数。
     */
    @Override
    public void asyncProcessRequest(ChannelHandlerContext ctx, RemotingCommand request, RemotingResponseCallback responseCallback) throws Exception {
        //调用asyncProcessRequest处理请求，然后调用thenAcceptAsync异步的执行回调
        asyncProcessRequest(ctx, request).thenAcceptAsync(responseCallback::callback, this.brokerController.getPutMessageFutureExecutor());
    }

    /**
     * asyncProcessRequest方法根据不同的RequestCode异步处理请求。
     * 如果RequestCode是CONSUMER_SEND_MSG_BACK，即消费者发送的消息回退请求，那么调用asyncConsumerSendMsgBack方法处理，其他情况下走默认处理逻辑。
     * 默认处理逻辑中，首先解析请求头，然后构建发送请求消息轨迹上下文，
     * 随后执行发送消息前钩子方法，最后判断如果是批量消息请求，
     * 那么调用asyncSendBatchMessage方法执行处理批量发送消息逻辑，否则调用asyncSendMessage方法处理其他发送消息逻辑，例如单条消息。
     *
     * @param ctx
     * @param request
     * @return
     * @throws RemotingCommandException
     */
    public CompletableFuture<RemotingCommand> asyncProcessRequest(ChannelHandlerContext ctx,
                                                                  RemotingCommand request) throws RemotingCommandException {
        final SendMessageContext mqtraceContext;
        /*
         * 根据不同的请求code选择不同的处理方式
         */
        switch (request.getCode()) {
            //如果是消费者发送的消息回退请求，该请求用于实现消息重试
            //如果消息消费失败，那么消息将被通过回退请求发送回broker，并延迟一段时间再消费
            case RequestCode.CONSUMER_SEND_MSG_BACK:
                return this.asyncConsumerSendMsgBack(ctx, request);
            //其他情况，都是属于生产者发送消息的请求，统一处理
            default:
                //解析请求头
                SendMessageRequestHeader requestHeader = parseRequestHeader(request);
                if (requestHeader == null) {
                    //如果请求头为null，那么返回一个null值结果
                    return CompletableFuture.completedFuture(null);
                }
                //构建发送请求消息轨迹上下文
                mqtraceContext = buildMsgContext(ctx, requestHeader);
                //执行发送消息前钩子方法
                this.executeSendMessageHookBefore(ctx, request, mqtraceContext);
                if (requestHeader.isBatch()) {
                    //处理批量发送消息逻辑
                    return this.asyncSendBatchMessage(ctx, request, mqtraceContext, requestHeader);
                } else {
                    //处理其他发送消息逻辑，例如单条消息,该方法是入口
                    return this.asyncSendMessage(ctx, request, mqtraceContext, requestHeader);
                }
        }
    }

    /**
     * SendMessageProcessor的方法
     * <p>
     * 是否需要拒绝处理该请求
     */
    @Override
    public boolean rejectRequest() {
        //检查操作系统页缓存PageCache是否繁忙或者检查临时存储池transientStorePool是否不足，如果其中有一个不满足要求，则拒绝处理该请求
        return this.brokerController.getMessageStore().isOSPageCacheBusy() ||
                /*
                 * 如果启用commitLog临时存储池，那么检查当前可用的buffers堆外内存的数量是否不足。
                 * RocketMQ中引入的 transientStorePoolEnable 能缓解 pagecache 的压力，
                 * 其原理是基于DirectByteBuffer和MappedByteBuffer的读写分离，消息先写入DirectByteBuffer（堆外内存），随后从MappedByteBuffer（pageCache）读取。
                 *
                 */
            this.brokerController.getMessageStore().isTransientStorePoolDeficient();
    }

    /**
     * SendMessageProcessor#asyncConsumerSendMsgBack方法用于处理消息回退请求。大概步骤为：
     *
     * 1. 前置校验。查找broker缓存的当前消费者组的订阅组配置SubscriptionGroupConfig，不存在订阅关系就直接返回，
     *      如果broker不支持写，那么直接返回。如果重试队列数量小于等于0，则直接返回，一般都是1。
     * 2. 根据consumerGroup获取对应的重试topic，这里仅仅是获取topic的名字%RETRY%+consumerGroup。随机选择一个重试队列id，一般都是0，因为重试队列数一般都是1。
     * 3. 调用createTopicInSendMessageBackMethod方法，尝试获取或者创建重试topic，其源码和创建普通topic差不多，
     *      区别就是重试topic不需要模板topic，默认读写队列数都是1，权限为读写，
     *      如果创建重试topic失败，直接返回。重试topic没有写的权限，直接返回。
     * 4. 调用lookMessageByOffset方法，根据消息物理偏移量从commitLog中找到该条消息。将属性RETRY_TOPIC的设置到消息属性中，该属性值为正常的topic。
     * 5. 从请求头中获取延迟等级。从订阅关系中获取最大重试次数，如果版本大于3.4.9，那么从请求头中获取最大重试次数，这是客户端传递过来的。
     *      并发消费模式默认最大16，顺序消费默认最大Integer.MAX_VALUE。
     * 6. 如果消息已重试次数 大于等于 最大重试次数，或者延迟等级小于0，那么消息不再重试，消息将会直接发往死信队列。
     *      6.1 获取该consumerGroup对应的死信队列topic，这里仅仅是获取topic的名字%DLQ%+consumerGroup。随机选择一个重试队列id，固定是0，因为死信队列数是1。
     *      6.2 尝试获取或者创建死信topic，实际上调用的调用获取重试topic的createTopicInSendMessageBackMethod方法，默认读写队列数都是1，权限为读写。
     *      6.3 设置消息延迟等级0，表示不会延迟，不进入延迟topic，直接发往死信队列。
     * 7. 如果没有达到最大重试次数，并且延迟等级不小于0，那么将会重试，因此设置延迟等级。
     *      7.1 如果参数中的delayLevel = 0，表示broker控制延迟等级，那么delayLevel = 3 + 已重试的次数 ，即默认从level3开始，即从延迟10s开始。
     *          如果参数中的delayLevel > 0，表示consumer控制延迟等级，那么参数是多少，等级就设置为多少。
     * 8. 创建内部消息对象MessageExtBrokerInner，设置相关属性。注意这里，设置的topic为重试topic或者死信topic。设置 消费次数 + 1。
     * 9. 调用asyncPutMessage方法，以异步方式处理、存储消息，将消息存储到commitLog中。该方法的源码我们在broker接收消息部分已经讲解过了。
     *      9.1 如果是延迟消息，即DelayTimeLevel大于0，那么替换topic为SCHEDULE_TOPIC_XXXX，替换queueId为延迟队列id， id = level - 1，
     *          如果延迟级别大于最大级别，则设置为最大级别18，，默认延迟2h。这些参数可以在broker端配置类MessageStoreConfig中配置。
     *      9.2 最后保存真实topic到消息的REAL_TOPIC属性，保存queueId到消息的REAL_QID属性，方便后面恢复。
     *          注意这里，如果保存的topic可能是重试topic，而真正的topic保存在RETRY_TOPIC属性中。
     *      9.3 broker后台定时任务服务ScheduleMessageService按照对应的延迟时间进行Delay后重新保存至“%RETRY%+consumerGroup”的重试队列中，然后即可被消费者重新消费。
     *
     * @param ctx
     * @param request
     * @return
     * @throws RemotingCommandException
     */
    private CompletableFuture<RemotingCommand> asyncConsumerSendMsgBack(ChannelHandlerContext ctx,
                                                                        RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        //解析请求头
        final ConsumerSendMsgBackRequestHeader requestHeader =
                (ConsumerSendMsgBackRequestHeader)request.decodeCommandCustomHeader(ConsumerSendMsgBackRequestHeader.class);
        String namespace = NamespaceUtil.getNamespaceFromResource(requestHeader.getGroup());
        //执行前置钩子
        if (this.hasConsumeMessageHook() && !UtilAll.isBlank(requestHeader.getOriginMsgId())) {
            ConsumeMessageContext context = buildConsumeMessageContext(namespace, requestHeader, request);
            this.executeConsumeMessageHookAfter(context);
        }
        /*
         * 1 前置校验
         */
        //查找broker缓存的当前消费者组的订阅组配置
        SubscriptionGroupConfig subscriptionGroupConfig =
            this.brokerController.getSubscriptionGroupManager().findSubscriptionGroupConfig(requestHeader.getGroup());
        //不存在订阅关系就直接返回
        if (null == subscriptionGroupConfig) {
            response.setCode(ResponseCode.SUBSCRIPTION_GROUP_NOT_EXIST);
            response.setRemark("subscription group not exist, " + requestHeader.getGroup() + " "
                + FAQUrl.suggestTodo(FAQUrl.SUBSCRIPTION_GROUP_NOT_EXIST));
            return CompletableFuture.completedFuture(response);
        }
        //如果broker不支持写，那么直接返回
        if (!PermName.isWriteable(this.brokerController.getBrokerConfig().getBrokerPermission())) {
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark("the broker[" + this.brokerController.getBrokerConfig().getBrokerIP1() + "] sending message is forbidden");
            return CompletableFuture.completedFuture(response);
        }

        //如果重试队列数量小于等于0，则直接返回，一般都是1
        if (subscriptionGroupConfig.getRetryQueueNums() <= 0) {
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
            return CompletableFuture.completedFuture(response);
        }

        /*
         * 2 根据consumerGroup获取对应的重试topic，这里仅仅是获取topic的名字
         *
         * RocketMQ会为每个消费组都设置一个Topic名称为“%RETRY%+consumerGroup”的重试队列
         * 这里需要注意的是，这里的重试队列是针对消费组，而不是针对每个Topic设置的
         * 每个Consumer实例在启动的时候就默认订阅了该消费组的重试队列Topic，但是只有在真正需要到重试topic的时候的才会创建
         */
        String newTopic = MixAll.getRetryTopic(requestHeader.getGroup());
        //随机选择一个重试队列id，一般都是0，因为重试队列数一般都是1
        int queueIdInt = ThreadLocalRandom.current().nextInt(99999999) % subscriptionGroupConfig.getRetryQueueNums();
        int topicSysFlag = 0;
        if (requestHeader.isUnitMode()) {
            topicSysFlag = TopicSysFlag.buildSysFlag(false, true);
        }

        /*
         * 3 尝试获取或者创建重试topic，其源码和创建普通topic差不多，区别就是重试topic不需要模板topic，默认读写队列数都是1，权限为读写
         */
        TopicConfig topicConfig = this.brokerController.getTopicConfigManager().createTopicInSendMessageBackMethod(
            newTopic,
            subscriptionGroupConfig.getRetryQueueNums(),
            PermName.PERM_WRITE | PermName.PERM_READ, topicSysFlag);
        //创建重试topic失败直接返回
        if (null == topicConfig) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("topic[" + newTopic + "] not exist");
            return CompletableFuture.completedFuture(response);
        }

        //重试topic没有写的权限，直接返回
        if (!PermName.isWriteable(topicConfig.getPerm())) {
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark(String.format("the topic[%s] sending message is forbidden", newTopic));
            return CompletableFuture.completedFuture(response);
        }
        /*
         * 4 根据消息物理偏移量从commitLog中找到该条消息
         */
        MessageExt msgExt = this.brokerController.getMessageStore().lookMessageByOffset(requestHeader.getOffset());
        //没找到消息，直接返回
        if (null == msgExt) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("look message by offset failed, " + requestHeader.getOffset());
            return CompletableFuture.completedFuture(response);
        }

        //从消息中获取重试topic属性RETRY_TOPIC
        final String retryTopic = msgExt.getProperty(MessageConst.PROPERTY_RETRY_TOPIC);
        //如果原消息没有该属性，则设置该属性值为正常的topic，表示第一次进行重试
        if (null == retryTopic) {
            MessageAccessor.putProperty(msgExt, MessageConst.PROPERTY_RETRY_TOPIC, msgExt.getTopic());
        }
        //配置是否需要等待存储完成后才返回，这里是否，即异步刷盘
        msgExt.setWaitStoreMsgOK(false);

        /*
         * 5 从请求头中获取延迟等级
         */
        int delayLevel = requestHeader.getDelayLevel();

        /*
         * 6 从订阅关系中获取最大重试次数
         * 如果版本大于3.4.9，那么从请求头中获取最大重试次数，这是客户端传递过来的
         * 并发消费模式最大16，顺序消费默认最大Integer.MAX_VALUE
         */
        int maxReconsumeTimes = subscriptionGroupConfig.getRetryMaxTimes();
        //如果版本大于3.4.9，那么从请求头中获取最大重试次数，这是客户端传递过来的
        //并发消费模式最大16，顺序消费默认最大Integer.MAX_VALUE
        if (request.getVersion() >= MQVersion.Version.V3_4_9.ordinal()) {
            Integer times = requestHeader.getMaxReconsumeTimes();
            if (times != null) {
                maxReconsumeTimes = times;
            }
        }

        /*
         * 7 如果消息已重试次数 大于等于 最大重试次数，或者延迟等级小于0，那么消息不再重试，直接发往死信队列
         */
        if (msgExt.getReconsumeTimes() >= maxReconsumeTimes
            || delayLevel < 0) {
            /*
             * 7.1 获取该consumerGroup对应的死信队列topic
             * RocketMQ会为每个消费组都设置一个Topic名称为%DLQ%+consumerGroup的死信队列topic
             * 这里需要注意的是，和重试队列一样，这里的死信队列是针对消费组，而不是针对每个Topic设置的。
             */
            newTopic = MixAll.getDLQTopic(requestHeader.getGroup());
            //随机选择一个死信队列id，这里是0，因为死信队列数DLQ_NUMS_PER_GROUP是1
            queueIdInt = ThreadLocalRandom.current().nextInt(99999999) % DLQ_NUMS_PER_GROUP;

            /*
             * 7.2 尝试获取或者创建死信topic，实际上调用的调用重试topic的方法，默认读写队列数都是1，权限为读写
             */
            topicConfig = this.brokerController.getTopicConfigManager().createTopicInSendMessageBackMethod(newTopic,
                    DLQ_NUMS_PER_GROUP,
                    PermName.PERM_WRITE | PermName.PERM_READ, 0);

            //创建死信topic失败直接返回
            if (null == topicConfig) {
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("topic[" + newTopic + "] not exist");
                return CompletableFuture.completedFuture(response);
            }
            //设置消息延迟等级0，表示不会延迟，不进入延迟topic
            msgExt.setDelayTimeLevel(0);
        }
        //没有达到最大重试次数，并且延迟等级不小于0
        else {
            //如果参数中的delayLevel = 0，表示broker控制延迟等级
            if (0 == delayLevel) {
                // 3 + 已重试的次数 ，即默认从level3开始，即从延迟10s开始
                delayLevel = 3 + msgExt.getReconsumeTimes();
            }
            //如果参数中的delayLevel > 0，表示consumer控制延迟等级
            //那么参数是多少，等级就设置为多少
            msgExt.setDelayTimeLevel(delayLevel);
        }

        /*
         * 8 创建内部消息对象MessageExtBrokerInner，设置相关属性
         */
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        /*
         *  注意这里，设置的topic为重试topic或者死信topic
         */
        msgInner.setTopic(newTopic);
        msgInner.setBody(msgExt.getBody());
        msgInner.setFlag(msgExt.getFlag());
        MessageAccessor.setProperties(msgInner, msgExt.getProperties());
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));
        msgInner.setTagsCode(MessageExtBrokerInner.tagsString2tagsCode(null, msgExt.getTags()));

        //设置队列id
        msgInner.setQueueId(queueIdInt);
        msgInner.setSysFlag(msgExt.getSysFlag());
        msgInner.setBornTimestamp(msgExt.getBornTimestamp());
        msgInner.setBornHost(msgExt.getBornHost());
        msgInner.setStoreHost(msgExt.getStoreHost());
        //设置 消费次数 + 1
        msgInner.setReconsumeTimes(msgExt.getReconsumeTimes() + 1);

        //原始消息id
        String originMsgId = MessageAccessor.getOriginMessageId(msgExt);
        //设置到PROPERTY_ORIGIN_MESSAGE_ID属性里面
        MessageAccessor.setOriginMessageId(msgInner, UtilAll.isBlank(originMsgId) ? msgExt.getMsgId() : originMsgId);
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));

        /*
         * 9 异步方式将消息存储到commitLog中
         *
         * 在该方法中，将会处理延迟消息的逻辑。如果是延迟消息，即DelayTimeLevel大于0
         * 那么替换topic为SCHEDULE_TOPIC_XXXX，替换queueId为延迟队列id， id = level - 1，保存真实topic和queueId，方便后面恢复。
         */
        CompletableFuture<PutMessageResult> putMessageResult = this.brokerController.getMessageStore().asyncPutMessage(msgInner);
        //结果处理
        return putMessageResult.thenApply(r -> {
            if (r != null) {
                switch (r.getPutMessageStatus()) {
                    case PUT_OK:
                        //重试topic或者死信topic
                        String backTopic = msgExt.getTopic();
                        //真实topic
                        String correctTopic = msgExt.getProperty(MessageConst.PROPERTY_RETRY_TOPIC);
                        if (correctTopic != null) {
                            backTopic = correctTopic;
                        }
                        //如果topic是RMQ_SYS_SCHEDULE_TOPIC，即延迟队列的topic，固定为 SCHEDULE_TOPIC_XXXX
                        if (TopicValidator.RMQ_SYS_SCHEDULE_TOPIC.equals(msgInner.getTopic())) {
                            //增加记录
                            this.brokerController.getBrokerStatsManager().incTopicPutNums(msgInner.getTopic());
                            this.brokerController.getBrokerStatsManager().incTopicPutSize(msgInner.getTopic(), r.getAppendMessageResult().getWroteBytes());
                            this.brokerController.getBrokerStatsManager().incQueuePutNums(msgInner.getTopic(), msgInner.getQueueId());
                            this.brokerController.getBrokerStatsManager().incQueuePutSize(msgInner.getTopic(), msgInner.getQueueId(), r.getAppendMessageResult().getWroteBytes());
                        }
                        this.brokerController.getBrokerStatsManager().incSendBackNums(requestHeader.getGroup(), backTopic);
                        response.setCode(ResponseCode.SUCCESS);
                        response.setRemark(null);
                        return response;
                    default:
                        break;
                }
                //异常情况
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark(r.getPutMessageStatus().name());
                return response;
            }
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("putMessageResult is null");
            return response;
        });
    }


    /**
     * 该方法是broker处理单条消息的通用入口方法，大概步骤为：
     *
     * 1. 调用preSend方法创建响应的命令对象，包括自动创建topic的逻辑，随后创建响应头对象。
     * 2. 随后创建MessageExtBrokerInner对象，从请求中获取消息的属性并设置到对象属性中，例如消息体，topic等等。
     * 3. 判断如果是重试或者死信消息，则调用handleRetryAndDLQ方法处理重试和死信队列消息，
     *      如果已重试次数大于最大重试次数，那么替换topic为死信队列topic，消息会被发送至死信队列。
     * 4. 判断如果是事务准备消息，并且不会拒绝处理事务消息，则调用asyncPrepareMessage方法以异步的方式处理、存储事务准备消息；
     * 5. 否则表示普通消息，调用asyncPutMessage方法处理、存储普通消息。
     *      asyncPutMessage以异步方式将消息存储到存储器中，处理器可以处理下一个请求而不是等待结果，当结果完成时，以异步方式通知客户端。
     * 6. 最后调用handlePutMessageResultFuture方法处理消息存储的处理结果
     *
     * @param ctx
     * @param request
     * @param mqtraceContext
     * @param requestHeader
     * @return
     */
    private CompletableFuture<RemotingCommand> asyncSendMessage(ChannelHandlerContext ctx, RemotingCommand request,
                                                                SendMessageContext mqtraceContext,
                                                                SendMessageRequestHeader requestHeader) {
        /*
         * 1 创建响应的命令对象，包括自动创建topic的逻辑
         */
        final RemotingCommand response = preSend(ctx, request, requestHeader);
        //获取响应头
        final SendMessageResponseHeader responseHeader = (SendMessageResponseHeader)response.readCustomHeader();

        if (response.getCode() != -1) {
            return CompletableFuture.completedFuture(response);
        }

        //获取消息体
        final byte[] body = request.getBody();

        int queueIdInt = requestHeader.getQueueId();
        //从broker的topicConfigTable缓存中根据topicName获取TopicConfig
        TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(requestHeader.getTopic());

        //如果队列id小于0，则随机选择一个写队列索引作为id
        if (queueIdInt < 0) {
            queueIdInt = randomQueueId(topicConfig.getWriteQueueNums());
        }

        //构建消息对象，保存着要存入commitLog的数据
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        //设置topic
        msgInner.setTopic(requestHeader.getTopic());
        //设置队列id
        msgInner.setQueueId(queueIdInt);

        /*
         * 2 处理重试和死信队列消息，将会对死信消息替换为死信topic
         */
        if (!handleRetryAndDLQ(requestHeader, response, request, msgInner, topicConfig)) {
            return CompletableFuture.completedFuture(response);
        }

        /*
         * 设置一系列属性
         */
        msgInner.setBody(body);
        msgInner.setFlag(requestHeader.getFlag());
        Map<String, String> origProps = MessageDecoder.string2messageProperties(requestHeader.getProperties());

        //设置到properties属性中
        MessageAccessor.setProperties(msgInner, origProps);
        msgInner.setBornTimestamp(requestHeader.getBornTimestamp());
        msgInner.setBornHost(ctx.channel().remoteAddress());
        msgInner.setStoreHost(this.getStoreHost());
        msgInner.setReconsumeTimes(requestHeader.getReconsumeTimes() == null ? 0 : requestHeader.getReconsumeTimes());
        String clusterName = this.brokerController.getBrokerConfig().getBrokerClusterName();
        MessageAccessor.putProperty(msgInner, MessageConst.PROPERTY_CLUSTER, clusterName);
        //WAIT属性表示 消息发送时是否等消息存储完成后再返回
        if (origProps.containsKey(MessageConst.PROPERTY_WAIT_STORE_MSG_OK)) {
            // There is no need to store "WAIT=true", remove it from propertiesString to save 9 bytes for each message.
            // It works for most case. In some cases msgInner.setPropertiesString invoked later and replace it.
            //不需要存储"WAIT=true"属性，从propertiesString中移除它，为每个消息节省9个字节。
            String waitStoreMsgOKValue = origProps.remove(MessageConst.PROPERTY_WAIT_STORE_MSG_OK);
            //将没有WAIT属性的origProps存入msgInner的propertiesString属性
            msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));
            // Reput to properties, since msgInner.isWaitStoreMsgOK() will be invoked later
            //将WAIT属性重新存入origProps集合中，因为msgInner.isWaitStoreMsgOK()稍后将被调用
            origProps.put(MessageConst.PROPERTY_WAIT_STORE_MSG_OK, waitStoreMsgOKValue);
        } else {
            //将没有WAIT属性的origProps存入msgInner的propertiesString属性
            msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));
        }

        CompletableFuture<PutMessageResult> putMessageResult = null;
        /*
         * 处理事务消息逻辑
         */
        //TRAN_MSG属性值为true，表示为事务消息
        String transFlag = origProps.get(MessageConst.PROPERTY_TRANSACTION_PREPARED);
        if (Boolean.parseBoolean(transFlag)) {
            //判断是否需要拒绝事务消息，如果需要拒绝，则返回NO_PERMISSION异常
            if (this.brokerController.getBrokerConfig().isRejectTransactionMessage()) {
                response.setCode(ResponseCode.NO_PERMISSION);
                response.setRemark(
                        "the broker[" + this.brokerController.getBrokerConfig().getBrokerIP1()
                                + "] sending transaction message is forbidden");
                return CompletableFuture.completedFuture(response);
            }
            //调用asyncPrepareMessage方法以异步的方式处理、存储事务准备消息，底层仍是asyncPutMessage方法
            putMessageResult = this.brokerController.getTransactionalMessageService().asyncPrepareMessage(msgInner);
        } else {
            //不是事务消息，那么调用asyncPutMessage方法处理，存储消息
            //以异步方式将消息存储到存储器中，处理器可以处理下一个请求而不是等待结果，当结果完成时，以异步方式通知客户端
            putMessageResult = this.brokerController.getMessageStore().asyncPutMessage(msgInner);
        }
        //处理消息存放的结果
        return handlePutMessageResultFuture(putMessageResult, response, request, msgInner, responseHeader, mqtraceContext, ctx, queueIdInt);
    }


    /**
     * SendMessageProcessor的方法
     * <p>
     * 处理消息存放结果
     *
     * @param putMessageResult   存放结果
     * @param response           响应对象
     * @param request            请求对象
     * @param msgInner           内部消息对象
     * @param responseHeader     响应头
     * @param sendMessageContext 发送消息上下文
     * @param ctx                连接上下文
     * @param queueIdInt         queueId
     * @return
     */
    private CompletableFuture<RemotingCommand> handlePutMessageResultFuture(CompletableFuture<PutMessageResult> putMessageResult,
                                                                            RemotingCommand response,
                                                                            RemotingCommand request,
                                                                            MessageExt msgInner,
                                                                            SendMessageResponseHeader responseHeader,
                                                                            SendMessageContext sendMessageContext,
                                                                            ChannelHandlerContext ctx,
                                                                            int queueIdInt) {
        //阻塞，当从存放消息完毕时，执行后续的操作，即执行handlePutMessageResult方法
        return putMessageResult.thenApply(r ->
            handlePutMessageResult(r, response, request, msgInner, responseHeader, sendMessageContext, ctx, queueIdInt)
        );
    }

    /**
     * 将会对重试消息进行检查，当已重试次数大于等于最大重试次数，那么消息将会发往死信队列，这里会将topic替换为死信队列的topic。
     *
     * SendMessageProcessor#asyncSendMessage方法最终还是会调用asyncPutMessage，以异步方式处理、存储消息
     *
     * @param requestHeader 请求头
     * @param response      响应命令对象
     * @param request       请求命令对象
     * @param msg           消息
     * @param topicConfig   topic配置
     * @return 是否是重试和死信队列消息
     */
    private boolean handleRetryAndDLQ(SendMessageRequestHeader requestHeader, RemotingCommand response,
                                      RemotingCommand request,
                                      MessageExt msg, TopicConfig topicConfig) {
        //获取topic
        String newTopic = requestHeader.getTopic();
        //如果是重试topic，顺序消费重试超过最大次数时发送的消息的topic就是重试topic
        if (null != newTopic && newTopic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
            String groupName = newTopic.substring(MixAll.RETRY_GROUP_TOPIC_PREFIX.length());
            //查找broker缓存的当前消费者组的订阅组配置
            SubscriptionGroupConfig subscriptionGroupConfig =
                this.brokerController.getSubscriptionGroupManager().findSubscriptionGroupConfig(groupName);
            if (null == subscriptionGroupConfig) {
                response.setCode(ResponseCode.SUBSCRIPTION_GROUP_NOT_EXIST);
                response.setRemark(
                    "subscription group not exist, " + groupName + " " + FAQUrl.suggestTodo(FAQUrl.SUBSCRIPTION_GROUP_NOT_EXIST));
                return false;
            }

            /*
             * 从订阅关系中获取最大重试次数
             * 如果版本大于3.4.9，那么从请求头中获取最大重试次数，这是客户端传递过来的
             * 并发消费模式最大16，顺序消费默认最大Integer.MAX_VALUE
             */
            int maxReconsumeTimes = subscriptionGroupConfig.getRetryMaxTimes();
            if (request.getVersion() >= MQVersion.Version.V3_4_9.ordinal() && requestHeader.getMaxReconsumeTimes() != null) {
                maxReconsumeTimes = requestHeader.getMaxReconsumeTimes();
            }
            //从请求头中获取已重试次数
            int reconsumeTimes = requestHeader.getReconsumeTimes() == null ? 0 : requestHeader.getReconsumeTimes();
            //如果已重试次数大于等于最大重试次数，那么消息将会发往死信队列
            if (reconsumeTimes >= maxReconsumeTimes) {
                /*
                 * 获取该consumerGroup对应的死信队列topic，名称为%DLQ%+consumerGroup
                 */
                newTopic = MixAll.getDLQTopic(groupName);
                //随机选择一个死信队列id，这里是0，因为死信队列数DLQ_NUMS_PER_GROUP是1
                int queueIdInt = ThreadLocalRandom.current().nextInt(99999999) % DLQ_NUMS_PER_GROUP;
                /*
                 * 尝试获取或者创建死信topic
                 */
                topicConfig = this.brokerController.getTopicConfigManager().createTopicInSendMessageBackMethod(newTopic,
                    DLQ_NUMS_PER_GROUP,
                    PermName.PERM_WRITE | PermName.PERM_READ, 0
                );
                //设置topic为死信队列topic
                msg.setTopic(newTopic);
                //设置队列id
                msg.setQueueId(queueIdInt);
                //设置消息延迟等级0，表示不会延迟，不进入延迟topic
                msg.setDelayTimeLevel(0);
                if (null == topicConfig) {
                    response.setCode(ResponseCode.SYSTEM_ERROR);
                    response.setRemark("topic[" + newTopic + "] not exist");
                    return false;
                }
            }
        }
        //系统标志
        int sysFlag = requestHeader.getSysFlag();
        if (TopicFilterType.MULTI_TAG == topicConfig.getTopicFilterType()) {
            sysFlag |= MessageSysFlag.MULTI_TAGS_FLAG;
        }
        msg.setSysFlag(sysFlag);
        return true;
    }

    private RemotingCommand sendMessage(final ChannelHandlerContext ctx,
                                        final RemotingCommand request,
                                        final SendMessageContext sendMessageContext,
                                        final SendMessageRequestHeader requestHeader) throws RemotingCommandException {

        final RemotingCommand response = RemotingCommand.createResponseCommand(SendMessageResponseHeader.class);
        final SendMessageResponseHeader responseHeader = (SendMessageResponseHeader)response.readCustomHeader();

        response.setOpaque(request.getOpaque());

        response.addExtField(MessageConst.PROPERTY_MSG_REGION, this.brokerController.getBrokerConfig().getRegionId());
        response.addExtField(MessageConst.PROPERTY_TRACE_SWITCH, String.valueOf(this.brokerController.getBrokerConfig().isTraceOn()));

        log.debug("receive SendMessage request command, {}", request);

        final long startTimstamp = this.brokerController.getBrokerConfig().getStartAcceptSendRequestTimeStamp();
        if (this.brokerController.getMessageStore().now() < startTimstamp) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(String.format("broker unable to service, until %s", UtilAll.timeMillisToHumanString2(startTimstamp)));
            return response;
        }

        response.setCode(-1);
        super.msgCheck(ctx, requestHeader, response);
        if (response.getCode() != -1) {
            return response;
        }

        final byte[] body = request.getBody();

        int queueIdInt = requestHeader.getQueueId();
        TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(requestHeader.getTopic());

        if (queueIdInt < 0) {
            queueIdInt = ThreadLocalRandom.current().nextInt(99999999) % topicConfig.getWriteQueueNums();
        }

        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setTopic(requestHeader.getTopic());
        msgInner.setQueueId(queueIdInt);

        if (!handleRetryAndDLQ(requestHeader, response, request, msgInner, topicConfig)) {
            return response;
        }

        msgInner.setBody(body);
        msgInner.setFlag(requestHeader.getFlag());
        MessageAccessor.setProperties(msgInner, MessageDecoder.string2messageProperties(requestHeader.getProperties()));
        msgInner.setBornTimestamp(requestHeader.getBornTimestamp());
        msgInner.setBornHost(ctx.channel().remoteAddress());
        msgInner.setStoreHost(this.getStoreHost());
        msgInner.setReconsumeTimes(requestHeader.getReconsumeTimes() == null ? 0 : requestHeader.getReconsumeTimes());
        String clusterName = this.brokerController.getBrokerConfig().getBrokerClusterName();
        MessageAccessor.putProperty(msgInner, MessageConst.PROPERTY_CLUSTER, clusterName);
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));
        PutMessageResult putMessageResult = null;
        Map<String, String> oriProps = MessageDecoder.string2messageProperties(requestHeader.getProperties());
        String traFlag = oriProps.get(MessageConst.PROPERTY_TRANSACTION_PREPARED);
        if (Boolean.parseBoolean(traFlag)
            && !(msgInner.getReconsumeTimes() > 0 && msgInner.getDelayTimeLevel() > 0)) { //For client under version 4.6.1
            if (this.brokerController.getBrokerConfig().isRejectTransactionMessage()) {
                response.setCode(ResponseCode.NO_PERMISSION);
                response.setRemark(
                    "the broker[" + this.brokerController.getBrokerConfig().getBrokerIP1()
                        + "] sending transaction message is forbidden");
                return response;
            }
            putMessageResult = this.brokerController.getTransactionalMessageService().prepareMessage(msgInner);
        } else {
            putMessageResult = this.brokerController.getMessageStore().putMessage(msgInner);
        }

        return handlePutMessageResult(putMessageResult, response, request, msgInner, responseHeader, sendMessageContext, ctx, queueIdInt);

    }

    private RemotingCommand handlePutMessageResult(PutMessageResult putMessageResult, RemotingCommand response,
                                                   RemotingCommand request, MessageExt msg,
                                                   SendMessageResponseHeader responseHeader, SendMessageContext sendMessageContext, ChannelHandlerContext ctx,
                                                   int queueIdInt) {

        //结果为null，那么直接返回系统异常
        if (putMessageResult == null) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("store putMessage return null");
            return response;
        }
        boolean sendOK = false;

        //解析存放消息状态码，转换为对应的响应码
        switch (putMessageResult.getPutMessageStatus()) {
            // Success
            case PUT_OK:
                sendOK = true;
                response.setCode(ResponseCode.SUCCESS);
                break;
            case FLUSH_DISK_TIMEOUT:
                response.setCode(ResponseCode.FLUSH_DISK_TIMEOUT);
                sendOK = true;
                break;
            case FLUSH_SLAVE_TIMEOUT:
                response.setCode(ResponseCode.FLUSH_SLAVE_TIMEOUT);
                sendOK = true;
                break;
            case SLAVE_NOT_AVAILABLE:
                response.setCode(ResponseCode.SLAVE_NOT_AVAILABLE);
                sendOK = true;
                break;

            // Failed
            case CREATE_MAPEDFILE_FAILED:
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("create mapped file failed, server is busy or broken.");
                break;
            case MESSAGE_ILLEGAL:
            case PROPERTIES_SIZE_EXCEEDED:
                response.setCode(ResponseCode.MESSAGE_ILLEGAL);
                response.setRemark(String.format("the message is illegal, maybe msg body or properties length not matched. msg body length limit %dB, msg properties length limit 32KB.",
                    this.brokerController.getMessageStoreConfig().getMaxMessageSize()));
                break;
            case SERVICE_NOT_AVAILABLE:
                response.setCode(ResponseCode.SERVICE_NOT_AVAILABLE);
                response.setRemark(
                    "service not available now. It may be caused by one of the following reasons: " +
                        "the broker's disk is full [" + diskUtil() + "], messages are put to the slave, message store has been shut down, etc.");
                break;
            case OS_PAGECACHE_BUSY:
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("[PC_SYNCHRONIZED]broker busy, start flow control for a while");
                break;
            case LMQ_CONSUME_QUEUE_NUM_EXCEEDED:
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("[LMQ_CONSUME_QUEUE_NUM_EXCEEDED]broker config enableLmq and enableMultiDispatch, lmq consumeQueue num exceed maxLmqConsumeQueueNum config num, default limit 2w.");
                break;
            case UNKNOWN_ERROR:
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("UNKNOWN_ERROR");
                break;
            default:
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("UNKNOWN_ERROR DEFAULT");
                break;
        }

        String owner = request.getExtFields().get(BrokerStatsManager.COMMERCIAL_OWNER);
        //如果发送成功
        if (sendOK) {

            //如果topic是SCHEDULE_TOPIC_XXXX，即延迟消息的topic
            if (TopicValidator.RMQ_SYS_SCHEDULE_TOPIC.equals(msg.getTopic())) {
                //增加统计计数
                this.brokerController.getBrokerStatsManager().incQueuePutNums(msg.getTopic(), msg.getQueueId(), putMessageResult.getAppendMessageResult().getMsgNum(), 1);
                this.brokerController.getBrokerStatsManager().incQueuePutSize(msg.getTopic(), msg.getQueueId(), putMessageResult.getAppendMessageResult().getWroteBytes());
            }

            //增加统计计数
            this.brokerController.getBrokerStatsManager().incTopicPutNums(msg.getTopic(), putMessageResult.getAppendMessageResult().getMsgNum(), 1);
            this.brokerController.getBrokerStatsManager().incTopicPutSize(msg.getTopic(),
                putMessageResult.getAppendMessageResult().getWroteBytes());
            this.brokerController.getBrokerStatsManager().incBrokerPutNums(putMessageResult.getAppendMessageResult().getMsgNum());

            response.setRemark(null);

            //设置响应头中的migId，实际上就是broker生成的offsetMsgId属性
            responseHeader.setMsgId(putMessageResult.getAppendMessageResult().getMsgId());
            //消息队列Id
            responseHeader.setQueueId(queueIdInt);
            //消息逻辑偏移量
            responseHeader.setQueueOffset(putMessageResult.getAppendMessageResult().getLogicsOffset());

            if (hasSendMessageHook()) {
                //如果有发送消息的钩子，那么执行
                sendMessageContext.setMsgId(responseHeader.getMsgId());
                sendMessageContext.setQueueId(responseHeader.getQueueId());
                sendMessageContext.setQueueOffset(responseHeader.getQueueOffset());

                int commercialBaseCount = brokerController.getBrokerConfig().getCommercialBaseCount();
                int wroteSize = putMessageResult.getAppendMessageResult().getWroteBytes();
                int incValue = (int)Math.ceil(wroteSize / BrokerStatsManager.SIZE_PER_COUNT) * commercialBaseCount;

                sendMessageContext.setCommercialSendStats(BrokerStatsManager.StatsType.SEND_SUCCESS);
                sendMessageContext.setCommercialSendTimes(incValue);
                sendMessageContext.setCommercialSendSize(wroteSize);
                sendMessageContext.setCommercialOwner(owner);
            }
            return response;
        } else {
            //如果有发送消息的钩子，那么执行
            if (hasSendMessageHook()) {
                int wroteSize = request.getBody().length;
                int incValue = (int)Math.ceil(wroteSize / BrokerStatsManager.SIZE_PER_COUNT);

                sendMessageContext.setCommercialSendStats(BrokerStatsManager.StatsType.SEND_FAILURE);
                sendMessageContext.setCommercialSendTimes(incValue);
                sendMessageContext.setCommercialSendSize(wroteSize);
                sendMessageContext.setCommercialOwner(owner);
            }
        }
        return response;
    }

    private CompletableFuture<RemotingCommand> asyncSendBatchMessage(ChannelHandlerContext ctx, RemotingCommand request,
                                                                     SendMessageContext mqtraceContext,
                                                                     SendMessageRequestHeader requestHeader) {
        final RemotingCommand response = preSend(ctx, request, requestHeader);
        final SendMessageResponseHeader responseHeader = (SendMessageResponseHeader)response.readCustomHeader();

        if (response.getCode() != -1) {
            return CompletableFuture.completedFuture(response);
        }

        int queueIdInt = requestHeader.getQueueId();
        TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(requestHeader.getTopic());

        if (queueIdInt < 0) {
            queueIdInt = randomQueueId(topicConfig.getWriteQueueNums());
        }

        if (requestHeader.getTopic().length() > Byte.MAX_VALUE) {
            response.setCode(ResponseCode.MESSAGE_ILLEGAL);
            response.setRemark("message topic length too long " + requestHeader.getTopic().length());
            return CompletableFuture.completedFuture(response);
        }

        MessageExtBatch messageExtBatch = new MessageExtBatch();
        messageExtBatch.setTopic(requestHeader.getTopic());
        messageExtBatch.setQueueId(queueIdInt);

        int sysFlag = requestHeader.getSysFlag();
        if (TopicFilterType.MULTI_TAG == topicConfig.getTopicFilterType()) {
            sysFlag |= MessageSysFlag.MULTI_TAGS_FLAG;
        }
        messageExtBatch.setSysFlag(sysFlag);

        messageExtBatch.setFlag(requestHeader.getFlag());
        MessageAccessor.setProperties(messageExtBatch, MessageDecoder.string2messageProperties(requestHeader.getProperties()));
        messageExtBatch.setBody(request.getBody());
        messageExtBatch.setBornTimestamp(requestHeader.getBornTimestamp());
        messageExtBatch.setBornHost(ctx.channel().remoteAddress());
        messageExtBatch.setStoreHost(this.getStoreHost());
        messageExtBatch.setReconsumeTimes(requestHeader.getReconsumeTimes() == null ? 0 : requestHeader.getReconsumeTimes());
        String clusterName = this.brokerController.getBrokerConfig().getBrokerClusterName();
        MessageAccessor.putProperty(messageExtBatch, MessageConst.PROPERTY_CLUSTER, clusterName);

        CompletableFuture<PutMessageResult> putMessageResult = this.brokerController.getMessageStore().asyncPutMessages(messageExtBatch);
        return handlePutMessageResultFuture(putMessageResult, response, request, messageExtBatch, responseHeader, mqtraceContext, ctx, queueIdInt);
    }

    public boolean hasConsumeMessageHook() {
        return consumeMessageHookList != null && !this.consumeMessageHookList.isEmpty();
    }

    public void executeConsumeMessageHookAfter(final ConsumeMessageContext context) {
        if (hasConsumeMessageHook()) {
            for (ConsumeMessageHook hook : this.consumeMessageHookList) {
                try {
                    hook.consumeMessageAfter(context);
                } catch (Throwable e) {
                    // Ignore
                }
            }
        }
    }

    @Override
    public SocketAddress getStoreHost() {
        return storeHost;
    }

    private String diskUtil() {
        double physicRatio = 100;
        String storePath;
        MessageStore messageStore = this.brokerController.getMessageStore();
        if (messageStore instanceof DefaultMessageStore) {
            storePath = ((DefaultMessageStore) messageStore).getStorePathPhysic();
        } else {
            storePath = this.brokerController.getMessageStoreConfig().getStorePathCommitLog();
        }
        String[] paths = storePath.trim().split(MessageStoreConfig.MULTI_PATH_SPLITTER);
        for (String storePathPhysic : paths) {
            physicRatio = Math.min(physicRatio, UtilAll.getDiskPartitionSpaceUsedPercent(storePathPhysic));
        }

        String storePathLogis =
            StorePathConfigHelper.getStorePathConsumeQueue(this.brokerController.getMessageStoreConfig().getStorePathRootDir());
        double logisRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathLogis);

        String storePathIndex =
            StorePathConfigHelper.getStorePathIndex(this.brokerController.getMessageStoreConfig().getStorePathRootDir());
        double indexRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathIndex);

        return String.format("CL: %5.2f CQ: %5.2f INDEX: %5.2f", physicRatio, logisRatio, indexRatio);
    }

    public void registerConsumeMessageHook(List<ConsumeMessageHook> consumeMessageHookList) {
        this.consumeMessageHookList = consumeMessageHookList;
    }

    static private ConsumeMessageContext buildConsumeMessageContext(String namespace,
                                                                    ConsumerSendMsgBackRequestHeader requestHeader,
                                                                    RemotingCommand request) {
        ConsumeMessageContext context = new ConsumeMessageContext();
        context.setNamespace(namespace);
        context.setConsumerGroup(requestHeader.getGroup());
        context.setTopic(requestHeader.getOriginTopic());
        context.setCommercialRcvStats(BrokerStatsManager.StatsType.SEND_BACK);
        context.setCommercialRcvTimes(1);
        context.setCommercialOwner(request.getExtFields().get(BrokerStatsManager.COMMERCIAL_OWNER));
        return context;
    }

    private int randomQueueId(int writeQueueNums) {
        return ThreadLocalRandom.current().nextInt(99999999) % writeQueueNums;
    }

    /**
     * 该方法用于创建响应的命令对象，其中还包括topic的校验，以及自动创建topic的逻辑。
     *
     * 该方法中将会创建一个RemotingCommand对象，并且设置唯一id为请求的id。
     * 除此之外还会校验如果当前时间小于该broker的起始服务时间，那么broker会返回一个SYSTEM_ERROR，表示现在broker还不能提供服务。
     *
     * 在最后，会调用msgCheck方法进行一系列的校验，包括自动创建topic的逻辑。
     *
     * 注意：
     * Producer发送消息源码的时候，我们的客户端，在发送消息的之前，会先选择一个topic所在的broker地址，如果topic不存在，那么选择默认topic的路由信息中的一个broker进行发送。
     * 当发送到broker之后，会发现没有指定的topic并且如果broker的autoCreateTopicEnable为true，那么将会自动创建topic，
     * 并且最后会马上调用registerBrokerAll方法向nameServer注册当前broker的新配置路由信息。
     * 生产者客户端会定时每30s从nameServer更新路由数据，
     * 如果此时有其他的producer的存在，并且刚好从nameServer获取到了这个新的topic的路由信息，
     * 假设其他producer也需要向该topic发送信息，由于发现topic路由信息已存在，并且只存在于刚才那一个broker中，此时这些producer都会将该topic的消息发送到这一个broker中来。
     * 这样，接下来所有的Producer都只会向这一个Broker发送消息，其他Broker也就不会再有机会创建新Topic。
     * 我们本想要该Topic在每个broker上都被自动创建，但结果仅仅是在一个broker上有该topic的信息，这样就背离了RocketMQ集群的初衷，不能实现压力的分摊。
     * 因此，RocketMQ官方建议生产环境下将broker的autoCreateTopicEnable设置为false，即关闭自动创建topic，全部改为界面手动在每个broker上创建，这样安全又保险。
     *
     *
     * @param ctx
     * @param request
     * @param requestHeader
     * @return
     */
    private RemotingCommand preSend(ChannelHandlerContext ctx, RemotingCommand request,
                                    SendMessageRequestHeader requestHeader) {
        //创建响应命令对象
        final RemotingCommand response = RemotingCommand.createResponseCommand(SendMessageResponseHeader.class);

        //设置唯一id为请求id
        response.setOpaque(request.getOpaque());

        //添加扩展字段属性"MSG_REGION"、"TRACE_ON"
        response.addExtField(MessageConst.PROPERTY_MSG_REGION, this.brokerController.getBrokerConfig().getRegionId());
        response.addExtField(MessageConst.PROPERTY_TRACE_SWITCH, String.valueOf(this.brokerController.getBrokerConfig().isTraceOn()));

        log.debug("Receive SendMessage request command {}", request);

        //获取配置的broker的处理请求的起始服务时间，默认为0
        final long startTimestamp = this.brokerController.getBrokerConfig().getStartAcceptSendRequestTimeStamp();

        //如果当前时间小于起始时间，那么broker会返回一个SYSTEM_ERROR，表示现在broker还不能提供服务
        if (this.brokerController.getMessageStore().now() < startTimestamp) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(String.format("broker unable to service, until %s", UtilAll.timeMillisToHumanString2(startTimestamp)));
            return response;
        }

        //设置code为-1
        response.setCode(-1);
        /*
         * 消息校验，包括自动创建topic的逻辑
         */
        super.msgCheck(ctx, requestHeader, response);

        return response;
    }
}
