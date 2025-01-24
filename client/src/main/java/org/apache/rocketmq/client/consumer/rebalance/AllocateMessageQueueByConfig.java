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
package org.apache.rocketmq.client.consumer.rebalance;

import java.util.List;
import org.apache.rocketmq.common.message.MessageQueue;

public class AllocateMessageQueueByConfig extends AbstractAllocateMessageQueueStrategy {
    private List<MessageQueue> messageQueueList;

    /**
     * 如果要想使用该策略，那么应该调用setMessageQueueList方法传入自定义的需要消费的消息队列集合，而allocate方法将直接返回该集合。
     *
     */
    @Override
    public List<MessageQueue> allocate(String consumerGroup, String currentCID, List<MessageQueue> mqAll,
        List<String> cidAll) {
        return this.messageQueueList;
    }

    @Override
    public String getName() {
        return "CONFIG";
    }

    public List<MessageQueue> getMessageQueueList() {
        return messageQueueList;
    }

    /**
     * 设置自定义的消息队列集合
     */
    public void setMessageQueueList(List<MessageQueue> messageQueueList) {
        this.messageQueueList = messageQueueList;
    }
}
