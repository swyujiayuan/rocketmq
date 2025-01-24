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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.client.consumer.PullCallback;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.hook.FilterMessageContext;
import org.apache.rocketmq.client.hook.FilterMessageHook;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.FindBrokerResult;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.common.sysflag.PullSysFlag;
import org.apache.rocketmq.remoting.exception.RemotingException;

public class PullAPIWrapper {
    private final InternalLogger log = ClientLogger.getLog();
    private final MQClientInstance mQClientFactory;
    private final String consumerGroup;
    private final boolean unitMode;
    private ConcurrentMap<MessageQueue, AtomicLong/* brokerId */> pullFromWhichNodeTable =
        new ConcurrentHashMap<MessageQueue, AtomicLong>(32);
    private volatile boolean connectBrokerByUser = false;
    private volatile long defaultBrokerId = MixAll.MASTER_ID;
    private Random random = new Random(System.currentTimeMillis());
    private ArrayList<FilterMessageHook> filterMessageHookList = new ArrayList<FilterMessageHook>();

    public PullAPIWrapper(MQClientInstance mQClientFactory, String consumerGroup, boolean unitMode) {
        this.mQClientFactory = mQClientFactory;
        this.consumerGroup = consumerGroup;
        this.unitMode = unitMode;
    }

    /**
     * 处理pullResult，进行消息解码、过滤以及设置其他属性的操作。
     *
     * 1. 更新下次拉取建议的brokerId，下次拉取消息时从pullFromWhichNodeTable中直接取出。
     * 2. 对消息二进制字节数组进行解码转换为java的List消息集合。
     * 3. 如果存在tag，并且不是classFilterMode，那么按照tag过滤消息，这就是客户端的消息过滤。这采用String#equals方法过滤，而broker端则是比较的tagHash值，即hashCode。
     * 4. 如果有消息过滤钩子，那么执行钩子方法，这里可以扩展自定义的消息过滤的逻辑。
     * 5. 遍历过滤通过的消息，设置属性。例如事务id，最大、最小偏移量、brokerName。
     * 6. 将过滤后的消息存入msgFoundList集合
     * 7. 因为消息已经被解析了，那么设置消息的字节数组为null，释放内存。
     *
     * @param mq               消息队列
     * @param pullResult       拉取结果
     * @param subscriptionData 获取topic对应的SubscriptionData订阅关系
     * @return 处理后的PullResult
     */
    public PullResult processPullResult(final MessageQueue mq, final PullResult pullResult,
        final SubscriptionData subscriptionData) {
        PullResultExt pullResultExt = (PullResultExt) pullResult;

        /*
         * 1 更新下次拉取建议的brokerId，下次拉取消息时从pullFromWhichNodeTable中直接取出
         */
        this.updatePullFromWhichNode(mq, pullResultExt.getSuggestWhichBrokerId());
        if (PullStatus.FOUND == pullResult.getPullStatus()) {
            /*
             * 2 对二进制字节数组进行解码转换为java的List<MessageExt>消息集合
             */
            ByteBuffer byteBuffer = ByteBuffer.wrap(pullResultExt.getMessageBinary());
            List<MessageExt> msgList = MessageDecoder.decodes(byteBuffer);

            List<MessageExt> msgListFilterAgain = msgList;

            /*
             * 3 如果存在tag，并且不是classFilterMode，那么按照tag过滤消息，这就是客户端的消息过滤
             */
            if (!subscriptionData.getTagsSet().isEmpty() && !subscriptionData.isClassFilterMode()) {
                msgListFilterAgain = new ArrayList<MessageExt>(msgList.size());
                for (MessageExt msg : msgList) {
                    if (msg.getTags() != null) {
                        //这采用String#equals方法过滤，而broker端则是比较的tagHash值，即hashCode
                        if (subscriptionData.getTagsSet().contains(msg.getTags())) {
                            msgListFilterAgain.add(msg);
                        }
                    }
                }
            }

            /*
             * 4 如果有消息过滤钩子，那么执行钩子方法，这里可以扩展自定义的消息过滤的逻辑
             */
            if (this.hasHook()) {
                FilterMessageContext filterMessageContext = new FilterMessageContext();
                filterMessageContext.setUnitMode(unitMode);
                filterMessageContext.setMsgList(msgListFilterAgain);
                this.executeHook(filterMessageContext);
            }

            /*
             * 5 遍历过滤通过的消息，设置属性
             */
            for (MessageExt msg : msgListFilterAgain) {
                //事务消息标识
                String traFlag = msg.getProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED);
                if (Boolean.parseBoolean(traFlag)) {
                    //如果是事务消息，则设置事务id
                    msg.setTransactionId(msg.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX));
                }
                //将响应中的最小和最大偏移量存入msg
                MessageAccessor.putProperty(msg, MessageConst.PROPERTY_MIN_OFFSET,
                    Long.toString(pullResult.getMinOffset()));
                MessageAccessor.putProperty(msg, MessageConst.PROPERTY_MAX_OFFSET,
                    Long.toString(pullResult.getMaxOffset()));
                //设置brokerName到msg
                msg.setBrokerName(mq.getBrokerName());
            }

            //将过滤后的消息存入msgFoundList集合
            pullResultExt.setMsgFoundList(msgListFilterAgain);
        }

        //6 因为消息已经被解析了，那么设置消息的字节数组为null，释放内存
        pullResultExt.setMessageBinary(null);

        return pullResult;
    }

    /**
     * PullAPIWrapper的方法
     * 更新下次拉取建议的brokerId
     *
     * @param mq       消息队列
     * @param brokerId 建议的brokerId
     */
    public void updatePullFromWhichNode(final MessageQueue mq, final long brokerId) {
        //存入pullFromWhichNodeTable集合
        AtomicLong suggest = this.pullFromWhichNodeTable.get(mq);
        if (null == suggest) {
            this.pullFromWhichNodeTable.put(mq, new AtomicLong(brokerId));
        } else {
            suggest.set(brokerId);
        }
    }

    public boolean hasHook() {
        return !this.filterMessageHookList.isEmpty();
    }

    public void executeHook(final FilterMessageContext context) {
        if (!this.filterMessageHookList.isEmpty()) {
            for (FilterMessageHook hook : this.filterMessageHookList) {
                try {
                    hook.filterMessage(context);
                } catch (Throwable e) {
                    log.error("execute hook error. hookName={}", hook.hookName());
                }
            }
        }
    }

    /**
     * PullAPIWrapper的方法
     *
     * 首先获取指定brokerName的broker地址，默认获取master地址，
     * 如果由建议的拉取地址，则获取建议的broker地址，没找到broker地址，那么更新topic路由信息再获取一次。
     *
     * 找到了broker之后，校验版本号，通过之后构造PullMessageRequestHeader请求头，
     * 然后调用MQClientAPIImpl#pullMessage方法发送请求，进行消息拉取。
     *
     * @param mq                         消息队列
     * @param subExpression              订阅关系表达式，它仅支持或操作，如“tag1 | | tag2 | | tag3”，如果为 null 或 *，则表示订阅全部
     * @param expressionType             订阅关系表达式类型，支持TAG和SQL92，用于过滤
     * @param subVersion                 订阅关系版本
     * @param offset                     下一个拉取的offset
     * @param maxNums                    一次批量拉取的最大消息数，默认32
     * @param sysFlag                    系统标记
     * @param commitOffset               提交的消费点位
     * @param brokerSuspendMaxTimeMillis broker挂起请求的最长时间，默认15s
     * @param timeoutMillis              消费者消息拉取超时时间，默认30s
     * @param communicationMode          消息拉取模式，默认为异步拉取
     * @param pullCallback               拉取到消息之后调用的回调函数
     * @return 拉取结果
     */
    public PullResult pullKernelImpl(
        final MessageQueue mq,
        final String subExpression,
        final String expressionType,
        final long subVersion,
        final long offset,
        final int maxNums,
        final int sysFlag,
        final long commitOffset,
        final long brokerSuspendMaxTimeMillis,
        final long timeoutMillis,
        final CommunicationMode communicationMode,
        final PullCallback pullCallback
    ) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {

        //获取指定brokerName的broker地址，默认获取master地址，如果由建议的拉取地址，则获取建议的broker地址
        FindBrokerResult findBrokerResult =
            this.mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(),
                this.recalculatePullFromWhichNode(mq), false);
        //没找到broker地址，那么更新topic路由信息再获取一次
        if (null == findBrokerResult) {
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
            findBrokerResult =
                this.mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(),
                    this.recalculatePullFromWhichNode(mq), false);
        }

        //找到了broker
        if (findBrokerResult != null) {
            {
                // check version
                //检查版本
                if (!ExpressionType.isTagType(expressionType)
                    && findBrokerResult.getBrokerVersion() < MQVersion.Version.V4_1_0_SNAPSHOT.ordinal()) {
                    throw new MQClientException("The broker[" + mq.getBrokerName() + ", "
                        + findBrokerResult.getBrokerVersion() + "] does not upgrade to support for filter message by " + expressionType, null);
                }
            }
            int sysFlagInner = sysFlag;

            if (findBrokerResult.isSlave()) {
                sysFlagInner = PullSysFlag.clearCommitOffsetFlag(sysFlagInner);
            }

            /*
             * 构造PullMessageRequestHeader请求头
             */
            PullMessageRequestHeader requestHeader = new PullMessageRequestHeader();
            //消费者组
            requestHeader.setConsumerGroup(this.consumerGroup);
            //topic
            requestHeader.setTopic(mq.getTopic());
            //队列id
            requestHeader.setQueueId(mq.getQueueId());
            //拉取偏移量
            requestHeader.setQueueOffset(offset);
            //最大拉取消息数量
            requestHeader.setMaxMsgNums(maxNums);
            //系统标记
            requestHeader.setSysFlag(sysFlagInner);
            //提交的消费点位
            requestHeader.setCommitOffset(commitOffset);
            //broker挂起请求的最长时间，默认15s
            requestHeader.setSuspendTimeoutMillis(brokerSuspendMaxTimeMillis);
            //订阅关系表达式，它仅支持或操作，如“tag1 | | tag2 | | tag3”，如果为 null 或 *，则表示订阅全部
            requestHeader.setSubscription(subExpression);
            //订阅关系版本
            requestHeader.setSubVersion(subVersion);
            //表达式类型 TAG 或者SQL92
            requestHeader.setExpressionType(expressionType);
            requestHeader.setBname(mq.getBrokerName());

            String brokerAddr = findBrokerResult.getBrokerAddr();
            if (PullSysFlag.hasClassFilterFlag(sysFlagInner)) {
                brokerAddr = computePullFromWhichFilterServer(mq.getTopic(), brokerAddr);
            }

            /*
             * 调用MQClientAPIImpl#pullMessage方法发送请求，进行消息拉取
             *
             */
            PullResult pullResult = this.mQClientFactory.getMQClientAPIImpl().pullMessage(
                brokerAddr,
                requestHeader,
                timeoutMillis,
                communicationMode,
                pullCallback);

            return pullResult;
        }

        throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
    }

    /**
     * 从建议的缓存中取brokerId
     * @param mq
     * @return
     */
    public long recalculatePullFromWhichNode(final MessageQueue mq) {
        if (this.isConnectBrokerByUser()) {
            return this.defaultBrokerId;
        }

        AtomicLong suggest = this.pullFromWhichNodeTable.get(mq);
        if (suggest != null) {
            return suggest.get();
        }

        return MixAll.MASTER_ID;
    }

    private String computePullFromWhichFilterServer(final String topic, final String brokerAddr)
        throws MQClientException {
        ConcurrentMap<String, TopicRouteData> topicRouteTable = this.mQClientFactory.getTopicRouteTable();
        if (topicRouteTable != null) {
            TopicRouteData topicRouteData = topicRouteTable.get(topic);
            List<String> list = topicRouteData.getFilterServerTable().get(brokerAddr);

            if (list != null && !list.isEmpty()) {
                return list.get(randomNum() % list.size());
            }
        }

        throw new MQClientException("Find Filter Server Failed, Broker Addr: " + brokerAddr + " topic: "
            + topic, null);
    }

    public boolean isConnectBrokerByUser() {
        return connectBrokerByUser;
    }

    public void setConnectBrokerByUser(boolean connectBrokerByUser) {
        this.connectBrokerByUser = connectBrokerByUser;

    }

    public int randomNum() {
        int value = random.nextInt();
        if (value < 0) {
            value = Math.abs(value);
            if (value < 0)
                value = 0;
        }
        return value;
    }

    public void registerFilterMessageHook(ArrayList<FilterMessageHook> filterMessageHookList) {
        this.filterMessageHookList = filterMessageHookList;
    }

    public long getDefaultBrokerId() {
        return defaultBrokerId;
    }

    public void setDefaultBrokerId(long defaultBrokerId) {
        this.defaultBrokerId = defaultBrokerId;
    }
}
