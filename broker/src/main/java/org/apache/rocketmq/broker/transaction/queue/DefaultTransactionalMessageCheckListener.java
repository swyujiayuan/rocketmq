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
package org.apache.rocketmq.broker.transaction.queue;

import org.apache.rocketmq.broker.transaction.AbstractTransactionalMessageCheckListener;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;

import java.util.concurrent.ThreadLocalRandom;

public class DefaultTransactionalMessageCheckListener extends AbstractTransactionalMessageCheckListener {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.TRANSACTION_LOGGER_NAME);

    public DefaultTransactionalMessageCheckListener() {
        super();
    }

    /**
     * 需要丢弃、跳过的消息，将会通过DefaultTransactionalMessageCheckListener# resolveDiscardMsg执行难丢弃的逻辑。
     *
     * 首先将half消息转换为内部消息对象，topic改为TRANS_CHECK_MAX_TIME_TOPIC，然后将消息存入该topic中，即算作丢弃完毕。
     *
     * 从这里可以知道，被丢弃的half消息就是存入了TRANS_CHECK_MAX_TIME_TOPIC这个内部topic中。
     *
     * @param msgExt Message to be discarded.
     */
    @Override
    public void resolveDiscardMsg(MessageExt msgExt) {
        log.error("MsgExt:{} has been checked too many times, so discard it by moving it to system topic TRANS_CHECK_MAXTIME_TOPIC", msgExt);

        try {
            //half消息转换为内部消息对象，topic为TRANS_CHECK_MAX_TIME_TOPIC
            MessageExtBrokerInner brokerInner = toMessageExtBrokerInner(msgExt);
            //将消息存入该topic
            PutMessageResult putMessageResult = this.getBrokerController().getMessageStore().putMessage(brokerInner);
            if (putMessageResult != null && putMessageResult.getPutMessageStatus() == PutMessageStatus.PUT_OK) {
                log.info("Put checked-too-many-time half message to TRANS_CHECK_MAXTIME_TOPIC OK. Restored in queueOffset={}, " +
                    "commitLogOffset={}, real topic={}", msgExt.getQueueOffset(), msgExt.getCommitLogOffset(), msgExt.getUserProperty(MessageConst.PROPERTY_REAL_TOPIC));
            } else {
                log.error("Put checked-too-many-time half message to TRANS_CHECK_MAXTIME_TOPIC failed, real topic={}, msgId={}", msgExt.getTopic(), msgExt.getMsgId());
            }
        } catch (Exception e) {
            log.warn("Put checked-too-many-time message to TRANS_CHECK_MAXTIME_TOPIC error. {}", e);
        }

    }

    /**
     * half消息转换为内部消息对象，topic为TRANS_CHECK_MAX_TIME_TOPIC，被丢弃的half消息将会存入这个这个固定的topic中，该topic队列数固定为1，具有读写权限。
     *
     * @param msgExt
     * @return
     */
    private MessageExtBrokerInner toMessageExtBrokerInner(MessageExt msgExt) {
        //创建或者获取topic信息，被丢弃的half消息将会存入TRANS_CHECK_MAX_TIME_TOPIC这个固定的topic
        TopicConfig topicConfig = this.getBrokerController().getTopicConfigManager().createTopicOfTranCheckMaxTime(TCMT_QUEUE_NUMS, PermName.PERM_READ | PermName.PERM_WRITE);
        //默认只有一个队列，所以queueId固定为0
        int queueId = ThreadLocalRandom.current().nextInt(99999999) % TCMT_QUEUE_NUMS;
        MessageExtBrokerInner inner = new MessageExtBrokerInner();
        inner.setTopic(topicConfig.getTopicName());
        inner.setBody(msgExt.getBody());
        inner.setFlag(msgExt.getFlag());
        MessageAccessor.setProperties(inner, msgExt.getProperties());
        inner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));
        inner.setTagsCode(MessageExtBrokerInner.tagsString2tagsCode(msgExt.getTags()));
        inner.setQueueId(queueId);
        inner.setSysFlag(msgExt.getSysFlag());
        inner.setBornHost(msgExt.getBornHost());
        inner.setBornTimestamp(msgExt.getBornTimestamp());
        inner.setStoreHost(msgExt.getStoreHost());
        inner.setReconsumeTimes(msgExt.getReconsumeTimes());
        inner.setMsgId(msgExt.getMsgId());
        inner.setWaitStoreMsgOK(false);
        return inner;
    }
}
