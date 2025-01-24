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

import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.common.message.MessageQueue;

/**
 * Average Hashing queue algorithm
 */
public class AllocateMessageQueueAveragely extends AbstractAllocateMessageQueueStrategy {

    /**
     * 平均分配策略，这是默认策略。尽量将消息队列平均分配给所有消费者，多余的队列分配至排在前面的消费者。
     * 分配的时候，前一个消费者分配完了，才会给下一个消费者分配。
     *
     * 计算消息队列数量与消费者数量的商，这个商就是每个消费者都会分到的队列数，然后对于余数，则只有排在前面的消费者能够分配到。
     *
     * 在进行分配的时候，只有当前一个消费者分配完了，才会分配下一个消费者。
     * 例如有消费者A、B，
     * 有5个消息队列1、2、3、4、5，计算得到A会分配3分队列，B会分配2个队列，那么将会先给消费者A分配1、2、3，再给消费者B分配到4、5。
     *
     * @param consumerGroup 当前consumerGroup
     * @param currentCID    当前currentCID
     * @param mqAll         当前topic的mq，已排序
     * @param cidAll        当前consumerGroup的clientId集合，已排序
     */
    @Override
    public List<MessageQueue> allocate(String consumerGroup, String currentCID, List<MessageQueue> mqAll,
        List<String> cidAll) {

        List<MessageQueue> result = new ArrayList<MessageQueue>();
        //参数校验
        if (!check(consumerGroup, currentCID, mqAll, cidAll)) {
            return result;
        }

        //当前currentCID在集合中的索引位置
        int index = cidAll.indexOf(currentCID);
        //计算平均分配后的余数，大于0表示不能被整除，必然有些消费者会多分配一个队列，有些消费者少分配一个队列
        int mod = mqAll.size() % cidAll.size();
        //计算当前消费者分配的队列数量
        //1、如果队列数量小于等于消费者数量，那么每个消费者最多只能分到一个队列，则算作1（后续还会计算），否则，表示每个消费者至少分配一个队列，需要继续计算
        //2、如果mod大于0并且当前消费者索引小于mod，那么当前消费者分到的队列数为平均分配的队列数+1，
        //  否则，分到的队列数为平均分配的队列数，即索引在余数范围内的，多分配一个
        int averageSize =
            mqAll.size() <= cidAll.size() ? 1 : (mod > 0 && index < mod ? mqAll.size() / cidAll.size()
                + 1 : mqAll.size() / cidAll.size());
        //如果mod大于0并且当前消费者索引小于mod，那么起始索引为index * averageSize，否则起始索引为index * averageSize + mod
        int startIndex = (mod > 0 && index < mod) ? index * averageSize : index * averageSize + mod;
        //最终分配的消息队列数量。取最小值是因为有些队列将会分配至较少的队列甚至无法分配到队列
        int range = Math.min(averageSize, mqAll.size() - startIndex);
        //分配队列，按照顺序分配
        for (int i = 0; i < range; i++) {
            result.add(mqAll.get((startIndex + i) % mqAll.size()));
        }
        return result;
    }

    @Override
    public String getName() {
        return "AVG";
    }
}
