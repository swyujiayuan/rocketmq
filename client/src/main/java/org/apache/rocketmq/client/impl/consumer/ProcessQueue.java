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
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.lang3.StringUtils;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.body.ProcessQueueInfo;

/**
 * Queue consumption snapshot
 */
public class ProcessQueue {
    public final static long REBALANCE_LOCK_MAX_LIVE_TIME =
        Long.parseLong(System.getProperty("rocketmq.client.rebalance.lockMaxLiveTime", "30000"));
    public final static long REBALANCE_LOCK_INTERVAL = Long.parseLong(System.getProperty("rocketmq.client.rebalance.lockInterval", "20000"));
    private final static long PULL_MAX_IDLE_TIME = Long.parseLong(System.getProperty("rocketmq.client.pull.pullMaxIdleTime", "120000"));
    private final InternalLogger log = ClientLogger.getLog();
    private final ReadWriteLock treeMapLock = new ReentrantReadWriteLock();
    private final TreeMap<Long, MessageExt> msgTreeMap = new TreeMap<Long, MessageExt>();
    private final AtomicLong msgCount = new AtomicLong();
    private final AtomicLong msgSize = new AtomicLong();
    private final Lock consumeLock = new ReentrantLock();
    /**
     * A subset of msgTreeMap, will only be used when orderly consume
     */
    private final TreeMap<Long, MessageExt> consumingMsgOrderlyTreeMap = new TreeMap<Long, MessageExt>();
    private final AtomicLong tryUnlockTimes = new AtomicLong(0);
    private volatile long queueOffsetMax = 0L;
    private volatile boolean dropped = false;
    private volatile long lastPullTimestamp = System.currentTimeMillis();
    private volatile long lastConsumeTimestamp = System.currentTimeMillis();
    private volatile boolean locked = false;
    private volatile long lastLockTimestamp = System.currentTimeMillis();
    private volatile boolean consuming = false;
    private volatile long msgAccCnt = 0;

    public boolean isLockExpired() {
        return (System.currentTimeMillis() - this.lastLockTimestamp) > REBALANCE_LOCK_MAX_LIVE_TIME;
    }

    public boolean isPullExpired() {
        return (System.currentTimeMillis() - this.lastPullTimestamp) > PULL_MAX_IDLE_TIME;
    }

    /**
     * 循环清理msgTreeMap中的过期消息，每次最多循环清理16条消息。
     *
     * 1. 每次循环首先获取msgTreeMap中的第一次元素的起始消费时间，msgTreeMap是一个红黑树，第一个节点就是offset最小的节点。
     * 2. 如果消费时间距离现在时间超过默认15min，那么获取这个msg，如果没有被消费，或者消费时间距离现在时间不超过默认15min，则结束循环。
     * 3. 将获取到的消息通过sendMessageBack发回broker延迟topic，将在给定延迟时间（默认从level 3，即10s开始）之后发回进行重试消费。
     * 4. 加锁判断如果这个消息还没有被消费完，并且还是在第一位，那么调用removeMessage方法从msgTreeMap中移除消息，进行下一轮判断。
     *
     */
    public void cleanExpiredMsg(DefaultMQPushConsumer pushConsumer) {
        //如果是顺序消费，直接返回，只有并发消费才会清理
        if (pushConsumer.getDefaultMQPushConsumerImpl().isConsumeOrderly()) {
            return;
        }

        //一次循环最多处理16个消息
        int loop = msgTreeMap.size() < 16 ? msgTreeMap.size() : 16;
        //遍历消息，最多处理前16个消息
        for (int i = 0; i < loop; i++) {
            MessageExt msg = null;
            try {
                //加锁
                this.treeMapLock.readLock().lockInterruptibly();
                try {
                    if (!msgTreeMap.isEmpty()) {
                        //获取msgTreeMap中的第一次元素的起始消费时间，msgTreeMap是一个红黑树，第一个节点就是offset最小的节点
                        String consumeStartTimeStamp = MessageAccessor.getConsumeStartTimeStamp(msgTreeMap.firstEntry().getValue());
                        //如果消费时间距离现在时间超过默认15min，那么获取这个msg
                        if (StringUtils.isNotEmpty(consumeStartTimeStamp) && System.currentTimeMillis() - Long.parseLong(consumeStartTimeStamp) > pushConsumer.getConsumeTimeout() * 60 * 1000) {
                            msg = msgTreeMap.firstEntry().getValue();
                        } else {
                            //如果没有被消费，或者消费时间距离现在时间不超过默认15min，则结束循环
                            break;
                        }
                    } else {
                        //msgTreeMap为空，结束循环
                        break;
                    }
                } finally {
                    this.treeMapLock.readLock().unlock();
                }
            } catch (InterruptedException e) {
                log.error("getExpiredMsg exception", e);
            }

            try {

                //将消息发回broker延迟topic，将在给定延迟时间（默认从level3，即10s开始）之后进行重试消费
                pushConsumer.sendMessageBack(msg, 3);
                log.info("send expire msg back. topic={}, msgId={}, storeHost={}, queueId={}, queueOffset={}", msg.getTopic(), msg.getMsgId(), msg.getStoreHost(), msg.getQueueId(), msg.getQueueOffset());
                try {
                    this.treeMapLock.writeLock().lockInterruptibly();
                    try {
                        //如果这个消息还没有被消费完，如果不是第一位，那就表示在处理的过程中该消息被消费了，无需删除，但是上面已经放进了延迟队里，该消息会被消费多一次
                        if (!msgTreeMap.isEmpty() && msg.getQueueOffset() == msgTreeMap.firstKey()) {
                            try {
                                //移除消息
                                removeMessage(Collections.singletonList(msg));
                            } catch (Exception e) {
                                log.error("send expired msg exception", e);
                            }
                        }
                    } finally {
                        this.treeMapLock.writeLock().unlock();
                    }
                } catch (InterruptedException e) {
                    log.error("getExpiredMsg exception", e);
                }
            } catch (Exception e) {
                log.error("send expired msg exception", e);
            }
        }
    }

    /**
     * 该方法将拉取到的所有消息，存入对应的processQueue处理队列内部的msgTreeMap中。
     *
     * 返回是否需要分发消费dispatchToConsume，当当前processQueue的内部的msgTreeMap中有消息并且consuming=false，即还没有开始消费时，将会返回true。
     *
     * dispatchToConsume对并发消费无影响，只对顺序消费有影响。
     *
     * @param msgs 一批消息
     * @return 是否需要分发消费，当当前processQueue的内部的msgTreeMap中有消息并且consuming=false，即还没有开始消费时，将会返回true
     */
    public boolean putMessage(final List<MessageExt> msgs) {
        boolean dispatchToConsume = false;
        try {
            //尝试加写锁防止并发
            this.treeMapLock.writeLock().lockInterruptibly();
            try {
                int validMsgCnt = 0;
                for (MessageExt msg : msgs) {
                    //当该消息的偏移量以及该消息存入msgTreeMap
                    MessageExt old = msgTreeMap.put(msg.getQueueOffset(), msg);
                    if (null == old) {
                        //如果集合没有这个offset的消息，那么增加统计数据
                        validMsgCnt++;
                        this.queueOffsetMax = msg.getQueueOffset();
                        msgSize.addAndGet(msg.getBody().length);
                    }
                }
                //消息计数
                msgCount.addAndGet(validMsgCnt);

                //当前processQueue的内部的msgTreeMap中有消息并且consuming=false，即还没有开始消费时，dispatchToConsume = true，consuming = true
                if (!msgTreeMap.isEmpty() && !this.consuming) {
                    dispatchToConsume = true;
                    this.consuming = true;
                }

                //计算broker累计消息数量
                if (!msgs.isEmpty()) {
                    MessageExt messageExt = msgs.get(msgs.size() - 1);
                    String property = messageExt.getProperty(MessageConst.PROPERTY_MAX_OFFSET);
                    if (property != null) {
                        long accTotal = Long.parseLong(property) - messageExt.getQueueOffset();
                        if (accTotal > 0) {
                            this.msgAccCnt = accTotal;
                        }
                    }
                }
            } finally {
                this.treeMapLock.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("putMessage exception", e);
        }

        return dispatchToConsume;
    }

    public long getMaxSpan() {
        try {
            this.treeMapLock.readLock().lockInterruptibly();
            try {
                if (!this.msgTreeMap.isEmpty()) {
                    return this.msgTreeMap.lastKey() - this.msgTreeMap.firstKey();
                }
            } finally {
                this.treeMapLock.readLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("getMaxSpan exception", e);
        }

        return 0;
    }

    /**
     * 从处理队列的msgTreeMap中将消费成功，以及消费失败但是发回broker成功的这批消息移除，然后返回msgTreeMap中的最小的消息偏移量。
     * 如果移除后msgTreeMap为空了，那么直接返回该处理队列记录的最大的消息偏移量+1。
     *
     * 这个返回的offset将会尝试用于更新在内存中的offsetTable中的最新偏移量信息，
     * 而offset除了在拉取消息时持久化之外，还会定时每5s调用persistAllConsumerOffset定时持久化
     *
     * @param msgs 需要被移除的消息集合
     * @return msgTreeMap中的最小的消息偏移量
     */
    public long removeMessage(final List<MessageExt> msgs) {
        long result = -1;
        final long now = System.currentTimeMillis();
        try {
            //获取锁
            this.treeMapLock.writeLock().lockInterruptibly();
            //更新时间戳
            this.lastConsumeTimestamp = now;
            try {
                //如果msgTreeMap存在数据
                if (!msgTreeMap.isEmpty()) {
                    //首先将result设置为该队列最大的消息偏移量+1
                    result = this.queueOffsetMax + 1;
                    int removedCnt = 0;
                    //遍历每一条消息尝试异常
                    for (MessageExt msg : msgs) {
                        MessageExt prev = msgTreeMap.remove(msg.getQueueOffset());
                        if (prev != null) {
                            removedCnt--;
                            msgSize.addAndGet(0 - msg.getBody().length);
                        }
                    }
                    msgCount.addAndGet(removedCnt);

                    //如果移除消息之后msgTreeMap不为空集合，那么result设置为msgTreeMap当前最小的消息偏移量
                    if (!msgTreeMap.isEmpty()) {
                        result = msgTreeMap.firstKey();
                    }
                }
            } finally {
                this.treeMapLock.writeLock().unlock();
            }
        } catch (Throwable t) {
            log.error("removeMessage exception", t);
        }

        return result;
    }

    public TreeMap<Long, MessageExt> getMsgTreeMap() {
        return msgTreeMap;
    }

    public AtomicLong getMsgCount() {
        return msgCount;
    }

    public AtomicLong getMsgSize() {
        return msgSize;
    }

    public boolean isDropped() {
        return dropped;
    }

    public void setDropped(boolean dropped) {
        this.dropped = dropped;
    }

    public boolean isLocked() {
        return locked;
    }

    public void setLocked(boolean locked) {
        this.locked = locked;
    }

    public void rollback() {
        try {
            this.treeMapLock.writeLock().lockInterruptibly();
            try {
                this.msgTreeMap.putAll(this.consumingMsgOrderlyTreeMap);
                this.consumingMsgOrderlyTreeMap.clear();
            } finally {
                this.treeMapLock.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("rollback exception", e);
        }
    }

    /**
     * 一次消费成功后，或者消费失败但是重试次数已经达到最大重试次数（可以通过DefaultMQPushConsumer#maxReconsumeTimes属性配置，
     * 默认无上限，即Integer.MAX_VALUE）。那么会执行ProcessQueue#commit方法。
     *
     * ProcessQueue#commit方法将会删除processQueue中消费完毕的消息，返回待更新的offset，或者说待提交的offset。大概步骤为：
     *
     * 1. 此前我们知道takeMessages方法会将拉取到的消息放入consumingMsgOrderlyTreeMap中，那么这里会先从该map中获取最大消息offset。
     * 2. 然后msgCount减去已经消费完成的消息数量，msgSize减去已经消费完成的消息大小，清空正在消费的消息consumingMsgOrderlyTreeMap。
     * 3. 最后返回下一个offset，也就是提交的待提交的offset，为已消费完毕的最大offset + 1。
     *
     * @return
     */
    public long commit() {
        try {
            this.treeMapLock.writeLock().lockInterruptibly();
            try {
                //获取正在消费的消息map中的最大消息offset
                Long offset = this.consumingMsgOrderlyTreeMap.lastKey();
                msgCount.addAndGet(0 - this.consumingMsgOrderlyTreeMap.size());
                //msgSize减去已经消费完成的消息大小
                for (MessageExt msg : this.consumingMsgOrderlyTreeMap.values()) {
                    msgSize.addAndGet(0 - msg.getBody().length);
                }
                //清空正在消费的消息map
                this.consumingMsgOrderlyTreeMap.clear();
                //返回下一个offset，已消费完毕的最大offset + 1
                if (offset != null) {
                    return offset + 1;
                }
            } finally {
                this.treeMapLock.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("commit exception", e);
        }

        return -1;
    }

    /**
     * 对于checkReconsumeTimes返回false的情况，即达到了最大重试次数但是sendMessageBack失败，或者没有达到最大重试次数，将会调用该方法，标记消息重新消费。
     *
     * 所谓标记，实际上很简单，就是将需要重复消费消息从正在消费的consumingMsgOrderlyTreeMap中移除，
     * 然后重新存入待消费的msgTreeMap中，那么将会在随后的消费中被拉取，进而实现重复消费。
     *
     * 所以说，并发消费的重复消费，需要将消息发往broker的重试topic中，等待再次拉取并重新消费，而顺序消费的重复消费就更加简单了，
     * 通过consumingMsgOrderlyTreeMap和msgTreeMap这两个map，实现直接在本地重试，不需要经过broker，直到达到了最大重试次数，
     * 才会通过sendMessageBack方法将消息发往broker，但是不会再被消费到了。

     *
     * @param msgs
     */
    public void makeMessageToConsumeAgain(List<MessageExt> msgs) {
        try {
            this.treeMapLock.writeLock().lockInterruptibly();
            try {
                //遍历消息
                for (MessageExt msg : msgs) {
                    //从正在消费的consumingMsgOrderlyTreeMap中移除该消息
                    this.consumingMsgOrderlyTreeMap.remove(msg.getQueueOffset());
                    //重新存入待消费的msgTreeMap中，那么将会在随后的消费中被拉取，进而实现重复消费
                    this.msgTreeMap.put(msg.getQueueOffset(), msg);
                }
            } finally {
                this.treeMapLock.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("makeMessageToCosumeAgain exception", e);
        }
    }

    public List<MessageExt> takeMessages(final int batchSize) {
        List<MessageExt> result = new ArrayList<MessageExt>(batchSize);
        final long now = System.currentTimeMillis();
        try {
            this.treeMapLock.writeLock().lockInterruptibly();
            this.lastConsumeTimestamp = now;
            try {
                if (!this.msgTreeMap.isEmpty()) {
                    for (int i = 0; i < batchSize; i++) {
                        Map.Entry<Long, MessageExt> entry = this.msgTreeMap.pollFirstEntry();
                        if (entry != null) {
                            result.add(entry.getValue());
                            consumingMsgOrderlyTreeMap.put(entry.getKey(), entry.getValue());
                        } else {
                            break;
                        }
                    }
                }

                if (result.isEmpty()) {
                    consuming = false;
                }
            } finally {
                this.treeMapLock.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("take Messages exception", e);
        }

        return result;
    }

    public boolean hasTempMessage() {
        try {
            this.treeMapLock.readLock().lockInterruptibly();
            try {
                return !this.msgTreeMap.isEmpty();
            } finally {
                this.treeMapLock.readLock().unlock();
            }
        } catch (InterruptedException e) {
        }

        return true;
    }

    public void clear() {
        try {
            this.treeMapLock.writeLock().lockInterruptibly();
            try {
                this.msgTreeMap.clear();
                this.consumingMsgOrderlyTreeMap.clear();
                this.msgCount.set(0);
                this.msgSize.set(0);
                this.queueOffsetMax = 0L;
            } finally {
                this.treeMapLock.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("rollback exception", e);
        }
    }

    public long getLastLockTimestamp() {
        return lastLockTimestamp;
    }

    public void setLastLockTimestamp(long lastLockTimestamp) {
        this.lastLockTimestamp = lastLockTimestamp;
    }

    public Lock getConsumeLock() {
        return consumeLock;
    }

    public long getLastPullTimestamp() {
        return lastPullTimestamp;
    }

    public void setLastPullTimestamp(long lastPullTimestamp) {
        this.lastPullTimestamp = lastPullTimestamp;
    }

    public long getMsgAccCnt() {
        return msgAccCnt;
    }

    public void setMsgAccCnt(long msgAccCnt) {
        this.msgAccCnt = msgAccCnt;
    }

    public long getTryUnlockTimes() {
        return this.tryUnlockTimes.get();
    }

    public void incTryUnlockTimes() {
        this.tryUnlockTimes.incrementAndGet();
    }

    public void fillProcessQueueInfo(final ProcessQueueInfo info) {
        try {
            this.treeMapLock.readLock().lockInterruptibly();

            if (!this.msgTreeMap.isEmpty()) {
                info.setCachedMsgMinOffset(this.msgTreeMap.firstKey());
                info.setCachedMsgMaxOffset(this.msgTreeMap.lastKey());
                info.setCachedMsgCount(this.msgTreeMap.size());
                info.setCachedMsgSizeInMiB((int) (this.msgSize.get() / (1024 * 1024)));
            }

            if (!this.consumingMsgOrderlyTreeMap.isEmpty()) {
                info.setTransactionMsgMinOffset(this.consumingMsgOrderlyTreeMap.firstKey());
                info.setTransactionMsgMaxOffset(this.consumingMsgOrderlyTreeMap.lastKey());
                info.setTransactionMsgCount(this.consumingMsgOrderlyTreeMap.size());
            }

            info.setLocked(this.locked);
            info.setTryUnlockTimes(this.tryUnlockTimes.get());
            info.setLastLockTimestamp(this.lastLockTimestamp);

            info.setDroped(this.dropped);
            info.setLastPullTimestamp(this.lastPullTimestamp);
            info.setLastConsumeTimestamp(this.lastConsumeTimestamp);
        } catch (Exception e) {
        } finally {
            this.treeMapLock.readLock().unlock();
        }
    }

    public long getLastConsumeTimestamp() {
        return lastConsumeTimestamp;
    }

    public void setLastConsumeTimestamp(long lastConsumeTimestamp) {
        this.lastConsumeTimestamp = lastConsumeTimestamp;
    }

}
