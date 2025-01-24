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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.FindBrokerResult;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.LockBatchRequestBody;
import org.apache.rocketmq.common.protocol.body.UnlockBatchRequestBody;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;

public abstract class RebalanceImpl {
    protected static final InternalLogger log = ClientLogger.getLog();
    protected final ConcurrentMap<MessageQueue, ProcessQueue> processQueueTable = new ConcurrentHashMap<MessageQueue, ProcessQueue>(64);
    protected final ConcurrentMap<String/* topic */, Set<MessageQueue>> topicSubscribeInfoTable =
        new ConcurrentHashMap<String, Set<MessageQueue>>();
    protected final ConcurrentMap<String /* topic */, SubscriptionData> subscriptionInner =
        new ConcurrentHashMap<String, SubscriptionData>();
    protected String consumerGroup;
    protected MessageModel messageModel;
    protected AllocateMessageQueueStrategy allocateMessageQueueStrategy;
    protected MQClientInstance mQClientFactory;

    public RebalanceImpl(String consumerGroup, MessageModel messageModel,
        AllocateMessageQueueStrategy allocateMessageQueueStrategy,
        MQClientInstance mQClientFactory) {
        this.consumerGroup = consumerGroup;
        this.messageModel = messageModel;
        this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
        this.mQClientFactory = mQClientFactory;
    }

    public void unlock(final MessageQueue mq, final boolean oneway) {
        FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(), MixAll.MASTER_ID, true);
        if (findBrokerResult != null) {
            UnlockBatchRequestBody requestBody = new UnlockBatchRequestBody();
            requestBody.setConsumerGroup(this.consumerGroup);
            requestBody.setClientId(this.mQClientFactory.getClientId());
            requestBody.getMqSet().add(mq);

            try {
                this.mQClientFactory.getMQClientAPIImpl().unlockBatchMQ(findBrokerResult.getBrokerAddr(), requestBody, 1000, oneway);
                log.warn("unlock messageQueue. group:{}, clientId:{}, mq:{}",
                    this.consumerGroup,
                    this.mQClientFactory.getClientId(),
                    mq);
            } catch (Exception e) {
                log.error("unlockBatchMQ exception, " + mq, e);
            }
        }
    }

    public void unlockAll(final boolean oneway) {
        HashMap<String, Set<MessageQueue>> brokerMqs = this.buildProcessQueueTableByBrokerName();

        for (final Map.Entry<String, Set<MessageQueue>> entry : brokerMqs.entrySet()) {
            final String brokerName = entry.getKey();
            final Set<MessageQueue> mqs = entry.getValue();

            if (mqs.isEmpty())
                continue;

            FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(brokerName, MixAll.MASTER_ID, true);
            if (findBrokerResult != null) {
                UnlockBatchRequestBody requestBody = new UnlockBatchRequestBody();
                requestBody.setConsumerGroup(this.consumerGroup);
                requestBody.setClientId(this.mQClientFactory.getClientId());
                requestBody.setMqSet(mqs);

                try {
                    this.mQClientFactory.getMQClientAPIImpl().unlockBatchMQ(findBrokerResult.getBrokerAddr(), requestBody, 1000, oneway);

                    for (MessageQueue mq : mqs) {
                        ProcessQueue processQueue = this.processQueueTable.get(mq);
                        if (processQueue != null) {
                            processQueue.setLocked(false);
                            log.info("the message queue unlock OK, Group: {} {}", this.consumerGroup, mq);
                        }
                    }
                } catch (Exception e) {
                    log.error("unlockBatchMQ exception, " + mqs, e);
                }
            }
        }
    }

    private HashMap<String/* brokerName */, Set<MessageQueue>> buildProcessQueueTableByBrokerName() {
        HashMap<String, Set<MessageQueue>> result = new HashMap<String, Set<MessageQueue>>(this.processQueueTable.size(), 1);
        for (MessageQueue mq : this.processQueueTable.keySet()) {
            Set<MessageQueue> mqs = result.get(mq.getBrokerName());
            if (null == mqs) {
                mqs = new HashSet<MessageQueue>();
                result.put(mq.getBrokerName(), mqs);
            }

            mqs.add(mq);
        }

        return result;
    }

    /**
     * 如果判断到某个消息队列是新分配给当前消费者的，并且如果是顺序消费，那么在当前消费者消费该消息队列之前，
     * 需要通过lock方法请求broker获取该队列的分布式锁。如果不是顺序消费，则此时不需要获取分布式锁。
     *
     * 如果获取分布式锁失败，那么不会为当前消息队列创建ProcessQueue和PullRequest，
     * 因为此时表示该消息队列还不属于当前消费者，不能进行消费，这是RocketMQ保证顺序消费防止重复消费的一个措施。
     *
     * 1. 该方法首先调用findBrokerAddressInSubscribe获取指定brokerName的master地址。
     * 2. 然后将当前消费者组、当前客户端id、当前需要被锁定的消息队列等信息封装为一个LockBatchRequestBody，最后向broker发送同步请求，Code为LOCK_BATCH_MQ。
     * 3. Broker返回一个set的MessageQueue集合，表示已经锁住的mq集合，然后编辑集合设置mq对应的processQueue属性，设置locked属性为true，设置加锁的时间属性为当前时间戳。
     * 4. 最后判断如果当前mq在集合中，那么返回true，表示当前mq锁定成功，否则返回false，表示锁定失败。
     *
     * @param mq
     * @return
     */
    public boolean lock(final MessageQueue mq) {
        //获取指定brokerName的master地址
        FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(), MixAll.MASTER_ID, true);
        if (findBrokerResult != null) {
            //构建获取分布式锁的请求体
            LockBatchRequestBody requestBody = new LockBatchRequestBody();
            //当前消费者组
            requestBody.setConsumerGroup(this.consumerGroup);
            //当前客户端id
            requestBody.setClientId(this.mQClientFactory.getClientId());
            //当前消息队列
            requestBody.getMqSet().add(mq);

            try {
                //向broker发送同步请求，Code为LOCK_BATCH_MQ，返回锁住的mq集合
                Set<MessageQueue> lockedMq =
                    this.mQClientFactory.getMQClientAPIImpl().lockBatchMQ(findBrokerResult.getBrokerAddr(), requestBody, 1000);
                //遍历锁住的mq集合
                for (MessageQueue mmqq : lockedMq) {
                    ProcessQueue processQueue = this.processQueueTable.get(mmqq);
                    if (processQueue != null) {
                        //设置locked为true
                        processQueue.setLocked(true);
                        processQueue.setLastLockTimestamp(System.currentTimeMillis());
                    }
                }

                //是否加锁成功
                boolean lockOK = lockedMq.contains(mq);
                log.info("the message queue lock {}, {} {}",
                    lockOK ? "OK" : "Failed",
                    this.consumerGroup,
                    mq);
                return lockOK;
            } catch (Exception e) {
                log.error("lockBatchMQ exception, " + mq, e);
            }
        }

        return false;
    }

    /**
     * 该方法定时每20s尝试锁定所有消息队列
     *
     * 1. 根据processQueueTable的数据，构建brokerName到其所有mq的map集合brokerMqs。
     *      在负载均衡并为当前消费者新分配消息队列的时候，也会对新分配的消息队列申请broker加锁，加锁成功后才会创建对应的processQueue存入processQueueTable。
     *      也就是说，如果是顺序消息，那么processQueueTable中的数据一定是曾经加锁成功了的。
     * 2. 遍历brokerMqs，调用MQClientAPIImpl#lockBatchMQ的方法，向broker发送同步请求，Code为LOCK_BATCH_MQ，请求批量锁定消息队列，返回锁住的mq集合。
     * 3. 遍历锁住的mq集合，获取对应的processQueue，设置processQueue的状态，设置locked为true，重新设置加锁的时间。遍历没有锁住的mq，设置locked为false。
     *
     */
    public void lockAll() {
        /*
         * 1 根据processQueueTable的数据，构建brokerName到所有mq的map集合
         * 在新分配消息队列的时候，也会对新分配的消息队列申请broker加锁，加锁成功后会创建对应的processQueue存入processQueueTable
         * 也就是说，如果是顺序消息，那么processQueueTable的数据一定是曾经加锁成功了的
         */
        HashMap<String, Set<MessageQueue>> brokerMqs = this.buildProcessQueueTableByBrokerName();

        Iterator<Entry<String, Set<MessageQueue>>> it = brokerMqs.entrySet().iterator();
        //遍历集合
        while (it.hasNext()) {
            Entry<String, Set<MessageQueue>> entry = it.next();
            final String brokerName = entry.getKey();
            final Set<MessageQueue> mqs = entry.getValue();

            if (mqs.isEmpty())
                continue;

            //获取指定brokerName的master地址。
            FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(brokerName, MixAll.MASTER_ID, true);
            if (findBrokerResult != null) {
                LockBatchRequestBody requestBody = new LockBatchRequestBody();
                requestBody.setConsumerGroup(this.consumerGroup);
                requestBody.setClientId(this.mQClientFactory.getClientId());
                requestBody.setMqSet(mqs);

                try {
                    /*
                     * 2 向broker发送同步请求，Code为LOCK_BATCH_MQ，请求批量锁定消息队列，返回锁住的mq集合
                     */
                    Set<MessageQueue> lockOKMQSet =
                        this.mQClientFactory.getMQClientAPIImpl().lockBatchMQ(findBrokerResult.getBrokerAddr(), requestBody, 1000);

                    //遍历锁住的mq集合
                    for (MessageQueue mq : lockOKMQSet) {
                        //获取对应的processQueue，设置processQueue的状态
                        ProcessQueue processQueue = this.processQueueTable.get(mq);
                        if (processQueue != null) {
                            if (!processQueue.isLocked()) {
                                log.info("the message queue locked OK, Group: {} {}", this.consumerGroup, mq);
                            }

                            //设置locked为true
                            processQueue.setLocked(true);
                            //设置加锁的时间
                            processQueue.setLastLockTimestamp(System.currentTimeMillis());
                        }
                    }
                    //遍历没有锁住的mq，设置locked为false
                    for (MessageQueue mq : mqs) {
                        if (!lockOKMQSet.contains(mq)) {
                            ProcessQueue processQueue = this.processQueueTable.get(mq);
                            if (processQueue != null) {
                                processQueue.setLocked(false);
                                log.warn("the message queue locked Failed, Group: {} {}", this.consumerGroup, mq);
                            }
                        }
                    }
                } catch (Exception e) {
                    log.error("lockBatchMQ exception, " + mqs, e);
                }
            }
        }
    }

    /**
     * RebalanceImpl的方法
     * <p>
     * 执行重平衡
     *
     * 该方法将会获取当前消费者的订阅信息集合，然后遍历订阅信息集合，获取订阅的topic，调用rebalanceByTopic方法对该topic进行重平衡。
     *
     * @param isOrder 是否顺序消费
     */
    public void doRebalance(final boolean isOrder) {
        //获取当前消费者的订阅信息集合
        Map<String, SubscriptionData> subTable = this.getSubscriptionInner();
        if (subTable != null) {
            //遍历订阅信息集合
            for (final Map.Entry<String, SubscriptionData> entry : subTable.entrySet()) {
                //获取topic
                final String topic = entry.getKey();
                try {
                    /*
                     * 对该topic进行重平衡
                     */
                    this.rebalanceByTopic(topic, isOrder);
                } catch (Throwable e) {
                    if (!topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                        log.warn("rebalanceByTopic Exception", e);
                    }
                }
            }
        }

        /*
         * 丢弃不属于当前消费者订阅的topic的队列快照ProcessQueue
         */
        this.truncateMessageQueueNotMyTopic();
    }

    public ConcurrentMap<String, SubscriptionData> getSubscriptionInner() {
        return subscriptionInner;
    }

    /**
     * 该方法根据topic进行重平衡，将会根据不同的消息模式执行不同的处理策略。
     *
     * 1. 如果是广播模式，广播模式下并没有负载均衡可言，每个consumer都会消费所有队列中的全部消息，
     *      仅仅是更新当前consumer的处理队列processQueueTable的信息。
     * 2. 如果是集群模式，首先基于负载均衡策略确定分配给当前消费者的MessageQueue，然后更新当前consumer的处理队列processQueueTable的信息。
     *
     * 集群模式的大概步骤为：
     * 1. 首先获取该topic的所有消息队列集合mqSet，随后从topic所在的broker中获取当前consumerGroup的clientId集合，
     *      即消费者客户端id集合cidAll。一个clientId代表一个消费者。
     * 2. 对topic的消息队列和clientId集合分别进行排序。排序能够保证，不同的客户端消费者在进行负载均衡时，其mqAll和cidAll中的元素顺序是一致的。
     * 3. 获取分配消息队列的策略实现AllocateMessageQueueStrategy，即负载均衡的策略类，执行allocate方法，
     *      为当前clientId也就是当前消费者，分配消息队列，这一步就是执行负载均衡或者说重平衡的算法。
     * 4. 调用updateProcessQueueTableInRebalance方法，更新新分配的消息队列的处理队列processQueueTable的信息，
     *      为新分配的消息队列创建最初的pullRequest并分发给PullMessageService。
     * 5. 如果processQueueTable发生了改变，那么调用messageQueueChanged方法。设置新的本地订阅关系版本，重设流控参数，立即给所有broker发送心跳，让Broker更新当前订阅关系。
     *
     * @param topic
     * @param isOrder
     */
    private void rebalanceByTopic(final String topic, final boolean isOrder) {
        //根据不同的消息模式执行不同的处理策略
        switch (messageModel) {
            /*
             * 广播模式的处理
             * 广播模式下并没有负载均衡可言，每个consumer都会消费所有队列中的全部消息，仅仅是更新当前consumer的处理队列processQueueTable的信息
             */
            case BROADCASTING: {
                //获取topic的消息队列
                Set<MessageQueue> mqSet = this.topicSubscribeInfoTable.get(topic);
                if (mqSet != null) {
                    /*
                     * 直接更新全部消息队列的处理队列processQueueTable的信息，创建最初的pullRequest并分发给PullMessageService
                     */
                    boolean changed = this.updateProcessQueueTableInRebalance(topic, mqSet, isOrder);
                    //如果processQueueTable发生了改变
                    if (changed) {
                        /*
                         * 设置新的本地订阅关系版本，重设流控参数，立即给所有broker发送心跳，让Broker更新当前订阅关系
                         */
                        this.messageQueueChanged(topic, mqSet, mqSet);
                        log.info("messageQueueChanged {} {} {} {}",
                            consumerGroup,
                            topic,
                            mqSet,
                            mqSet);
                    }
                } else {
                    log.warn("doRebalance, {}, but the topic[{}] not exist.", consumerGroup, topic);
                }
                break;
            }
            /*
             * 集群模式的处理
             * 基于负载均衡策略确定跟配给当前消费者的MessageQueue，然后更新当前consumer的处理队列processQueueTable的信息
             */
            case CLUSTERING: {
                //获取topic的消息队列
                Set<MessageQueue> mqSet = this.topicSubscribeInfoTable.get(topic);
                /*
                 * 从topic所在的broker中获取当前consumerGroup的clientId集合，即消费者客户端id集合
                 * 一个clientId代表一个消费者
                 */
                List<String> cidAll = this.mQClientFactory.findConsumerIdList(topic, consumerGroup);
                if (null == mqSet) {
                    if (!topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                        log.warn("doRebalance, {}, but the topic[{}] not exist.", consumerGroup, topic);
                    }
                }

                if (null == cidAll) {
                    log.warn("doRebalance, {} {}, get consumer id list failed", consumerGroup, topic);
                }

                if (mqSet != null && cidAll != null) {
                    //将topic的消息队列存入list集合中
                    List<MessageQueue> mqAll = new ArrayList<MessageQueue>();
                    mqAll.addAll(mqSet);

                    /*
                     * 对topic的消息队列和clientId集合分别进行排序
                     * 排序能够保证，不同的客户端消费者在进行负载均衡时，其mqAll和cidAll中的元素顺序是一致的
                     */
                    Collections.sort(mqAll);
                    Collections.sort(cidAll);

                    //获取分配消息队列的策略实现，即负载均衡的策略类
                    AllocateMessageQueueStrategy strategy = this.allocateMessageQueueStrategy;

                    List<MessageQueue> allocateResult = null;
                    try {
                        /*
                         * AllocateMessageQueueStrategy#allocate方法为当前clientId也就是当前消费者，分配消息队列，这一步实际上就是执行负载均衡或者说重平衡的算法。
                         * AllocateMessageQueueStrategy是RocketMQ消费者之间消息分配的策略算法接口，RocketMQ已经提供了非常多的算法策略实现类，
                         * 同时我们自己也可以通过实现AllocateMessageQueueStrategy接口定义自己的负载均衡策略。
                         * 注意，在执行负载均衡策略之前，已经对消息队列和消费者进行了排序，因此不同的消费者客户端得到的顺序应该是一致的。
                         * RocketMQ内置了六个负载均衡策略的实现类
                         * AllocateMessageQueueAveragely：平均分配策略，这是默认策略
                         * AllocateMessageQueueAveragelyByCircle：环形平均分配策略
                         * AllocateMessageQueueByConfig：根据用户配置的消息队列分配
                         * AllocateMessageQueueByMachineRoom：机房平均分配策略
                         * AllocateMachineRoomNearby：机房就近分配策略
                         * AllocateMessageQueueConsistentHash：一致性哈希分配策
                         */
                        allocateResult = strategy.allocate(
                            this.consumerGroup,
                            this.mQClientFactory.getClientId(),
                            mqAll,
                            cidAll);
                    } catch (Throwable e) {
                        log.error("AllocateMessageQueueStrategy.allocate Exception. allocateMessageQueueStrategyName={}", strategy.getName(),
                            e);
                        return;
                    }

                    //对消息队列去重
                    Set<MessageQueue> allocateResultSet = new HashSet<MessageQueue>();
                    if (allocateResult != null) {
                        allocateResultSet.addAll(allocateResult);
                    }

                    /*
                     * 更新新分配的消息队列的处理队列processQueueTable的信息，创建最初的pullRequest并分发给PullMessageService
                     */
                    boolean changed = this.updateProcessQueueTableInRebalance(topic, allocateResultSet, isOrder);
                    //如果processQueueTable发生了改变
                    if (changed) {
                        log.info(
                            "rebalanced result changed. allocateMessageQueueStrategyName={}, group={}, topic={}, clientId={}, mqAllSize={}, cidAllSize={}, rebalanceResultSize={}, rebalanceResultSet={}",
                            strategy.getName(), consumerGroup, topic, this.mQClientFactory.getClientId(), mqSet.size(), cidAll.size(),
                            allocateResultSet.size(), allocateResultSet);
                        /*
                         * 设置新的本地订阅关系版本，重设流控参数，立即给所有broker发送心跳，让Broker更新当前订阅关系
                         */
                        this.messageQueueChanged(topic, mqSet, allocateResultSet);
                    }
                }
                break;
            }
            default:
                break;
        }
    }

    private void truncateMessageQueueNotMyTopic() {
        Map<String, SubscriptionData> subTable = this.getSubscriptionInner();

        for (MessageQueue mq : this.processQueueTable.keySet()) {
            if (!subTable.containsKey(mq.getTopic())) {

                ProcessQueue pq = this.processQueueTable.remove(mq);
                if (pq != null) {
                    pq.setDropped(true);
                    log.info("doRebalance, {}, truncateMessageQueueNotMyTopic remove unnecessary mq, {}", consumerGroup, mq);
                }
            }
        }
    }

    /**
     * 在通过AllocateMessageQueueStrategy#allocate的负载均衡算法为当前消费者分配了新的消息队列之后，
     * 需要调用updateProcessQueueTableInRebalance方法，更新新分配的消息队列的处理队列processQueueTable的信息，
     * 创建最初的pullRequest并分发给PullMessageService。
     *
     * 该方法非常的重要，大概步骤为：
     * 1. 遍历当前消费者已分配的所有处理队列processQueueTable，当消费者启动并且第一次执行该方法时，processQueueTable是一个空集合。
     *      如果当前遍历到的消息队列和当前topic相等：
     *      1.1 如果新分配的消息队列集合不包含当前遍历到的消息队列，说明这个队列被移除了。
     *          1.1.1 设置对应的处理队列dropped = true，该队列中的消息将不会被消费。
     *          1.1.2 调用removeUnnecessaryMessageQueue删除不必要的消息队列。删除成功后，processQueueTable移除该条目，changed置为true。
     *      1.2 如果当前遍历到的处理队列最后一次拉取消息的时间距离现在超过120s，那么算作消费超时，可能是没有新消息或者网络通信失败。
     *          1.2.1 如果是push消费模式，设置对应的处理队列dropped = true，该队列中的消息将不会被消费。
     *              调用removeUnnecessaryMessageQueue删除不必要的消息队列。删除成功后，processQueueTable移除该条目，changed置为true。
     * 2. 创建一个pullRequestList集合，用于存放新增的PullRequest。遍历新分配的消息队列集合，
     *      如果当前消费者的处理队列集合processQueueTable中不包含该消息队列，那么表示这个消息队列是新分配的，需要进行一系列处理：
     *      2.1 如果是顺序消费，并且调用lock方法请求broker锁定该队列失败，即获取该队列的分布式锁失败表示新增消息队列失败，
     *          这个队列可能还再被其他消费者消费，那么本次重平衡就不再消费该队列，进入下次循环。
     *      2.2 如果不是顺序消费或者顺序消费加锁成功，调用removeDirtyOffset方法从offsetTable中移除该消息队列的消费点位offset记录信息。
     *      2.3 为该消息队列创建一个处理队列ProcessQueue。
     *      2.4 调用computePullFromWhereWithException方法，获取该MessageQueue的下一个消息的消费偏移量nextOffset，pull模式返回0，
     *          push模式则根据consumeFromWhere计算得到。
     *      2.5 如果nextOffset大于0，表示获取消费位点成功。保存当前消息队列MessageQueue和处理队列ProcessQueue关系到processQueueTable。
     *      2.6 新建一个PullRequest，设置对应的offset、consumerGroup、mq、pq的信息，并且存入pullRequestList集合中。
     *          这里就是最初产生拉取消息请求的地方。changed置为true。
     * 3. 调用dispatchPullRequest方法，分发本次创建的PullRequest请求。
     *      3.1 pull模式需要手动拉取消息，这些请求会作废，因此该方法是一个空实现。
     *      3.2 push模式下自动拉取消息，而这里的PullRequest就是对应的消息队列的第一个拉取请求，
     *          因此这些请求会被PullMessageService依次处理，后续实现自动拉取消息。这里就是push模式下最初的产生拉取消息请求的地方。
     *
     * @param topic   订阅的主题
     * @param mqSet   新分配的消息队列集合
     * @param isOrder 是否顺序消费
     * @return 是否有变更
     */
    private boolean updateProcessQueueTableInRebalance(final String topic, final Set<MessageQueue> mqSet,
        final boolean isOrder) {
        boolean changed = false;

        /*
         * 遍历当前消费者的所有处理队列，当消费者启动并且第一次执行该方法时，processQueueTable是一个空集合
         */
        Iterator<Entry<MessageQueue, ProcessQueue>> it = this.processQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<MessageQueue, ProcessQueue> next = it.next();
            //key为消息队列
            MessageQueue mq = next.getKey();
            //value为对应的处理队列
            ProcessQueue pq = next.getValue();

            //如果topic相等
            if (mq.getTopic().equals(topic)) {
                //如果新分配的消息队列集合不包含当前遍历到的消息队列，说明这个队列被移除了
                if (!mqSet.contains(mq)) {
                    //设置对应的处理队列dropped = true，该队列中的消息将不会被消费
                    pq.setDropped(true);
                    /*
                     * 删除不必要的消息队列
                     */
                    if (this.removeUnnecessaryMessageQueue(mq, pq)) {
                        //删除成功后，移除该条目，changed置为true
                        it.remove();
                        changed = true;
                        log.info("doRebalance, {}, remove unnecessary mq, {}", consumerGroup, mq);
                    }
                    //如果处理队列最后一次拉取消息的时间距离现在超过120s，那么算作消费超时，可能是没有新消息或者网络通信失败
                } else if (pq.isPullExpired()) {
                    switch (this.consumeType()) {
                        case CONSUME_ACTIVELY:
                            break;
                        //如果是push消费模式
                        case CONSUME_PASSIVELY:
                            //设置对应的处理队列dropped = true，该队列中的消息将不会被消费
                            pq.setDropped(true);
                            /*
                             * 删除不必要的消息队列
                             */
                            if (this.removeUnnecessaryMessageQueue(mq, pq)) {
                                //删除成功后，移除该条目，changed置为true
                                it.remove();
                                changed = true;
                                log.error("[BUG]doRebalance, {}, remove unnecessary mq, {}, because pull is pause, so try to fixed it",
                                    consumerGroup, mq);
                            }
                            break;
                        default:
                            break;
                    }
                }
            }
        }

        /*
         * 遍历新分配的消息队列集合
         */
        List<PullRequest> pullRequestList = new ArrayList<PullRequest>();
        for (MessageQueue mq : mqSet) {
            //如果当前消费者的处理队列集合中不包含该消息队列，那么表示这个消息队列是新分配的
            if (!this.processQueueTable.containsKey(mq)) {
                //如果是顺序消费，并且请求broker锁定该队列失败，即获取该队列的分布式锁失败
                //表示新增消息队列失败，这个队列可能还再被其他消费者消费，那么本次重平衡就不再消费该队列
                if (isOrder && !this.lock(mq)) {
                    log.warn("doRebalance, {}, add a new mq failed, {}, because lock failed", consumerGroup, mq);
                    continue;
                }

                //从offsetTable中移除该消息队列的消费点位offset记录信息
                this.removeDirtyOffset(mq);
                /*
                 * 为该消息队列创建一个处理队列
                 */
                ProcessQueue pq = new ProcessQueue();
                pq.setLocked(true);

                long nextOffset = -1L;
                try {
                    /*
                     * 获取该MessageQueue的下一个消息的消费偏移量offset
                     * pull模式返回0，push模式则根据consumeFromWhere计算得到
                     * 对于新分配的mq，需要知道从哪个点位开始消费，computePullFromWhereWithException方法就是用来获取该MessageQueue的下一个消息的消费偏移量offset的
                     */
                    nextOffset = this.computePullFromWhereWithException(mq);
                } catch (Exception e) {
                    log.info("doRebalance, {}, compute offset failed, {}", consumerGroup, mq);
                    continue;
                }

                // 如果nextOffset大于0，表示获取消费位点成功
                if (nextOffset >= 0) {
                    //保存当前消息队列MessageQueue和处理队列ProcessQueue关系
                    ProcessQueue pre = this.processQueueTable.putIfAbsent(mq, pq);
                    if (pre != null) {
                        log.info("doRebalance, {}, mq already exists, {}", consumerGroup, mq);
                    } else {
                        /*
                         * 新建一个PullRequest，设置对应的offset、consumerGroup、mq、pq的信息，并且存入pullRequestList集合中
                         * 这里就是最初产生拉取消息请求的地方
                         */
                        log.info("doRebalance, {}, add a new mq, {}", consumerGroup, mq);
                        PullRequest pullRequest = new PullRequest();
                        pullRequest.setConsumerGroup(consumerGroup);
                        pullRequest.setNextOffset(nextOffset);
                        pullRequest.setMessageQueue(mq);
                        pullRequest.setProcessQueue(pq);
                        pullRequestList.add(pullRequest);
                        //changed置为true
                        changed = true;
                    }
                } else {
                    log.warn("doRebalance, {}, add new mq failed, {}", consumerGroup, mq);
                }
            }
        }

        /*
         * 分发本次创建的PullRequest请求。
         * pull模式需要手动拉取消息，这些请求会作废，因此该方法是一个空实现
         * push模式下自动拉取消息，而这里的PullRequest就是对应的消息队列的第一个拉取请求，因此这些请求会被PullMessageService依次处理，后续实现自动拉取消息
         */
        this.dispatchPullRequest(pullRequestList);

        return changed;
    }

    public abstract void messageQueueChanged(final String topic, final Set<MessageQueue> mqAll,
        final Set<MessageQueue> mqDivided);

    public abstract boolean removeUnnecessaryMessageQueue(final MessageQueue mq, final ProcessQueue pq);

    public abstract ConsumeType consumeType();

    public abstract void removeDirtyOffset(final MessageQueue mq);

    /**
     * When the network is unstable, using this interface may return wrong offset.
     * It is recommended to use computePullFromWhereWithException instead.
     * @param mq
     * @return offset
     */
    @Deprecated
    public abstract long computePullFromWhere(final MessageQueue mq);

    public abstract long computePullFromWhereWithException(final MessageQueue mq) throws MQClientException;

    public abstract void dispatchPullRequest(final List<PullRequest> pullRequestList);

    public void removeProcessQueue(final MessageQueue mq) {
        ProcessQueue prev = this.processQueueTable.remove(mq);
        if (prev != null) {
            boolean droped = prev.isDropped();
            prev.setDropped(true);
            this.removeUnnecessaryMessageQueue(mq, prev);
            log.info("Fix Offset, {}, remove unnecessary mq, {} Droped: {}", consumerGroup, mq, droped);
        }
    }

    public ConcurrentMap<MessageQueue, ProcessQueue> getProcessQueueTable() {
        return processQueueTable;
    }

    public ConcurrentMap<String, Set<MessageQueue>> getTopicSubscribeInfoTable() {
        return topicSubscribeInfoTable;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public MessageModel getMessageModel() {
        return messageModel;
    }

    public void setMessageModel(MessageModel messageModel) {
        this.messageModel = messageModel;
    }

    public AllocateMessageQueueStrategy getAllocateMessageQueueStrategy() {
        return allocateMessageQueueStrategy;
    }

    public void setAllocateMessageQueueStrategy(AllocateMessageQueueStrategy allocateMessageQueueStrategy) {
        this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
    }

    public MQClientInstance getmQClientFactory() {
        return mQClientFactory;
    }

    public void setmQClientFactory(MQClientInstance mQClientFactory) {
        this.mQClientFactory = mQClientFactory;
    }

    public void destroy() {
        Iterator<Entry<MessageQueue, ProcessQueue>> it = this.processQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<MessageQueue, ProcessQueue> next = it.next();
            next.getValue().setDropped(true);
        }

        this.processQueueTable.clear();
    }
}
