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
package org.apache.rocketmq.broker.out;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.broker.latency.BrokerFixedThreadPoolExecutor;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.common.DataVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.namesrv.RegisterBrokerResult;
import org.apache.rocketmq.common.namesrv.TopAddressing;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.body.ConsumerOffsetSerializeWrapper;
import org.apache.rocketmq.common.protocol.body.KVTable;
import org.apache.rocketmq.common.protocol.body.RegisterBrokerBody;
import org.apache.rocketmq.common.protocol.body.SubscriptionGroupWrapper;
import org.apache.rocketmq.common.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.common.protocol.header.namesrv.QueryDataVersionRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.QueryDataVersionResponseHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.RegisterBrokerRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.RegisterBrokerResponseHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.UnRegisterBrokerRequestHeader;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.RemotingClient;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyRemotingClient;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class BrokerOuterAPI {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private final RemotingClient remotingClient;
    private final TopAddressing topAddressing = new TopAddressing(MixAll.getWSAddr());
    private String nameSrvAddr = null;
    private BrokerFixedThreadPoolExecutor brokerOuterExecutor = new BrokerFixedThreadPoolExecutor(4, 10, 1, TimeUnit.MINUTES,
        new ArrayBlockingQueue<>(32), new ThreadFactoryImpl("brokerOutApi_thread_", true));

    public BrokerOuterAPI(final NettyClientConfig nettyClientConfig) {
        this(nettyClientConfig, null);
    }

    public BrokerOuterAPI(final NettyClientConfig nettyClientConfig, RPCHook rpcHook) {
        this.remotingClient = new NettyRemotingClient(nettyClientConfig);
        this.remotingClient.registerRPCHook(rpcHook);
    }

    public void start() {
        this.remotingClient.start();
    }

    public void shutdown() {
        this.remotingClient.shutdown();
        this.brokerOuterExecutor.shutdown();
    }

    public String fetchNameServerAddr() {
        try {
            String addrs = this.topAddressing.fetchNSAddr();
            if (addrs != null && !UtilAll.isBlank(addrs)) {
                if (!addrs.equals(this.nameSrvAddr)) {
                    log.info("name server address changed, old: {} new: {}", this.nameSrvAddr, addrs);
                    this.updateNameServerAddressList(addrs);
                    this.nameSrvAddr = addrs;
                    return nameSrvAddr;
                }
            }
        } catch (Exception e) {
            log.error("fetchNameServerAddr Exception", e);
        }
        return nameSrvAddr;
    }

    public void updateNameServerAddressList(final String addrs) {
        List<String> lst = new ArrayList<String>();
        String[] addrArray = addrs.split(";");
        for (String addr : addrArray) {
            lst.add(addr);
        }

        this.remotingClient.updateNameServerAddressList(lst);
    }

    /**
     * 内部调用BrokerOuterAPI#registerBrokerAll方法，BrokerOuterAPI类专门提供了broker向外部发起请求的api方法。
     *
     * 因为broker要向所有的nameServer进行注册，为了提升性能，registerBrokerAll方法里面使用了多线程机制，
     * 使用brokerOuterExecutor线程池并行的发起对于每个nameserver的注册请求。
     *
     * 有了多线程提升效率，自然就需要保证线程安全和控制并发：
     *
     * 1. 因为需要在多线程中将执行结果并行存入集合中， RocketMQ使用了CopyOnWriteArrayList这个并发集合来保证线程安全。
     * CopyOnWriteArrayList采用COW（Copy On Write）机制，即写是复制，读数据时完全没有控制，即不会加锁。写数据时加独占锁，
     * 并且会复制出一个新的List，在新的List中写入数据，写完了之后使用新的List替换旧的List。
     *
     * 2. 虽然对于注册的请求使用了线程池异步执行，但是主线程却需要等待这些请求都执行完毕，
     * 所有的结果才能继续向下执行，对于这种并发控制， RocketMQ使用了CountDownLatch倒计数器，它能够使得主线程阻塞，
     * 确保在其他线程任务执行完毕之后，才会唤醒主线程继续执行后续逻辑。
     *
     * @param clusterName
     * @param brokerAddr
     * @param brokerName
     * @param brokerId
     * @param haServerAddr
     * @param topicConfigWrapper
     * @param filterServerList
     * @param oneway
     * @param timeoutMills
     * @param compressed
     * @return
     */
    public List<RegisterBrokerResult> registerBrokerAll(
        final String clusterName,
        final String brokerAddr,
        final String brokerName,
        final long brokerId,
        final String haServerAddr,
        final TopicConfigSerializeWrapper topicConfigWrapper,
        final List<String> filterServerList,
        final boolean oneway,
        final int timeoutMills,
        final boolean compressed) {

        //创建一个CopyOnWriteArrayList类型的集合，用来保存请求的返回结果
        final List<RegisterBrokerResult> registerBrokerResultList = new CopyOnWriteArrayList<>();
        //获取全部nameServer地址
        List<String> nameServerAddressList = this.remotingClient.getNameServerAddressList();
        if (nameServerAddressList != null && nameServerAddressList.size() > 0) {

            /*
             * 构造请求头，将一些broker信息放入请求头
             */
            final RegisterBrokerRequestHeader requestHeader = new RegisterBrokerRequestHeader();
            requestHeader.setBrokerAddr(brokerAddr);
            requestHeader.setBrokerId(brokerId);
            requestHeader.setBrokerName(brokerName);
            requestHeader.setClusterName(clusterName);
            requestHeader.setHaServerAddr(haServerAddr);
            requestHeader.setCompressed(compressed);

            /*
             * 构造请求体，将携带topic信息的topicConfigTable，以及版本信息的dataVersion，以及消费过滤信息集合放入请求体
             */
            RegisterBrokerBody requestBody = new RegisterBrokerBody();
            requestBody.setTopicConfigSerializeWrapper(topicConfigWrapper);
            requestBody.setFilterServerList(filterServerList);
            final byte[] body = requestBody.encode(compressed);
            final int bodyCrc32 = UtilAll.crc32(body);
            requestHeader.setBodyCrc32(bodyCrc32);
            /*
             * 使用CountDownLatch作为倒计数器，用于并发控制
             * CountDownLatch可以让主线程等待，直到任务全部执行完毕之后，再唤醒主线程继续后面的逻辑
             */
            final CountDownLatch countDownLatch = new CountDownLatch(nameServerAddressList.size());
            /*
             * 采用线程池的方式，即多线程并发的向所有的nameserver发起注册请求
             */
            for (final String namesrvAddr : nameServerAddressList) {
                //并发的执行线程任务
                brokerOuterExecutor.execute(() -> {
                    try {
                        //发起注册请求
                        RegisterBrokerResult result = registerBroker(namesrvAddr, oneway, timeoutMills, requestHeader, body);
                        if (result != null) {
                            registerBrokerResultList.add(result);
                        }

                        log.info("register broker[{}]to name server {} OK", brokerId, namesrvAddr);
                    } catch (Exception e) {
                        log.warn("registerBroker Exception, {}", namesrvAddr, e);
                    } finally {
                        /*
                         * 每一个请求执行完毕，无论是正常还是异常，都需要减少一个计数
                         */
                        countDownLatch.countDown();
                    }
                });
            }

            try {
                /*
                 * 主线程在此限时等待6000ms，直到上面的任务全部执行完毕之后，计数变为0，会唤醒主线程继续后面的逻辑
                 */
                countDownLatch.await(timeoutMills, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
            }
        }

        return registerBrokerResultList;
    }

    /**
     * broker注册请求为同步请求，code为REGISTER_BROKER，103，
     * 注册的信息主要包括自身的所有topic数据、dataVersion、filterServerList、
     * 以及包括集群名、broker地址、broker名、brokerId等等在内的一些broker自身的信息。

     *
     * @param namesrvAddr
     * @param oneway
     * @param timeoutMills
     * @param requestHeader
     * @param body
     * @return
     */
    private RegisterBrokerResult registerBroker(
        final String namesrvAddr,
        final boolean oneway,
        final int timeoutMills,
        final RegisterBrokerRequestHeader requestHeader,
        final byte[] body
    ) throws RemotingCommandException, MQBrokerException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException,
        InterruptedException {
        //构建远程调用请求对象，code为REGISTER_BROKER，103
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.REGISTER_BROKER, requestHeader);
        request.setBody(body);

        //如果是单向请求，则broker发起异步请求即可返回，不必关心执行结果，注册请求不是单向请求
        if (oneway) {
            try {
                this.remotingClient.invokeOneway(namesrvAddr, request, timeoutMills);
            } catch (RemotingTooMuchRequestException e) {
                // Ignore
            }
            return null;
        }

        /*
         * 通过remotingClient发起同步调用，非单向请求，即需要同步的获取结果
         */
        RemotingCommand response = this.remotingClient.invokeSync(namesrvAddr, request, timeoutMills);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                /*
                 * 解析响应数据，封装结果
                 */
                RegisterBrokerResponseHeader responseHeader =
                    (RegisterBrokerResponseHeader) response.decodeCommandCustomHeader(RegisterBrokerResponseHeader.class);
                RegisterBrokerResult result = new RegisterBrokerResult();
                result.setMasterAddr(responseHeader.getMasterAddr());
                result.setHaServerAddr(responseHeader.getHaServerAddr());
                if (response.getBody() != null) {
                    result.setKvTable(KVTable.decode(response.getBody(), KVTable.class));
                }
                return result;
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark(), requestHeader == null ? null : requestHeader.getBrokerAddr());
    }

    public void unregisterBrokerAll(
        final String clusterName,
        final String brokerAddr,
        final String brokerName,
        final long brokerId
    ) {
        List<String> nameServerAddressList = this.remotingClient.getNameServerAddressList();
        if (nameServerAddressList != null) {
            for (String namesrvAddr : nameServerAddressList) {
                try {
                    this.unregisterBroker(namesrvAddr, clusterName, brokerAddr, brokerName, brokerId);
                    log.info("unregisterBroker OK, NamesrvAddr: {}", namesrvAddr);
                } catch (Exception e) {
                    log.warn("unregisterBroker Exception, {}", namesrvAddr, e);
                }
            }
        }
    }

    public void unregisterBroker(
        final String namesrvAddr,
        final String clusterName,
        final String brokerAddr,
        final String brokerName,
        final long brokerId
    ) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQBrokerException {
        UnRegisterBrokerRequestHeader requestHeader = new UnRegisterBrokerRequestHeader();
        requestHeader.setBrokerAddr(brokerAddr);
        requestHeader.setBrokerId(brokerId);
        requestHeader.setBrokerName(brokerName);
        requestHeader.setClusterName(clusterName);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UNREGISTER_BROKER, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(namesrvAddr, request, 3000);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return;
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark(), brokerAddr);
    }

    /**
     * 向所有nameServer发起请求（请求code为QUERY_DATA_VERSION，322），
     * 获取所有nameserver的DataVersion数据，然后一一对比自身的DataVersion数据是否一致
     *
     * @param clusterName
     * @param brokerAddr
     * @param brokerName
     * @param brokerId
     * @param topicConfigWrapper
     * @param timeoutMills
     * @return
     */
    public List<Boolean> needRegister(
        final String clusterName,
        final String brokerAddr,
        final String brokerName,
        final long brokerId,
        final TopicConfigSerializeWrapper topicConfigWrapper,
        final int timeoutMills) {
        //创建一个CopyOnWriteArrayList类型的集合，用来保存请求的返回结果
        final List<Boolean> changedList = new CopyOnWriteArrayList<>();
        //获取全部nameServer地址
        List<String> nameServerAddressList = this.remotingClient.getNameServerAddressList();
        if (nameServerAddressList != null && nameServerAddressList.size() > 0) {
            final CountDownLatch countDownLatch = new CountDownLatch(nameServerAddressList.size());
            for (final String namesrvAddr : nameServerAddressList) {
                brokerOuterExecutor.execute(() -> {
                    try {
                        /*
                         * 构造请求头，将一些broker信息放入请求头
                         */
                        QueryDataVersionRequestHeader requestHeader = new QueryDataVersionRequestHeader();
                        requestHeader.setBrokerAddr(brokerAddr);
                        requestHeader.setBrokerId(brokerId);
                        requestHeader.setBrokerName(brokerName);
                        requestHeader.setClusterName(clusterName);
                        //构建远程调用请求对象，code为QUERY_DATA_VERSION，322
                        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_DATA_VERSION, requestHeader);
                        request.setBody(topicConfigWrapper.getDataVersion().encode());
                        RemotingCommand response = remotingClient.invokeSync(namesrvAddr, request, timeoutMills);
                        DataVersion nameServerDataVersion = null;
                        Boolean changed = false;
                        switch (response.getCode()) {
                            case ResponseCode.SUCCESS: {
                                QueryDataVersionResponseHeader queryDataVersionResponseHeader =
                                    (QueryDataVersionResponseHeader) response.decodeCommandCustomHeader(QueryDataVersionResponseHeader.class);
                                changed = queryDataVersionResponseHeader.getChanged();
                                byte[] body = response.getBody();
                                if (body != null) {
                                    //获取nameserver的dataversion
                                    //DataVersion是RocketMQ的数据版本控制机制
                                    //DataVersion#nextVersion方法被调用时，将会引起timestamp和counter的改变，一般来说，当新创建broker，或者更新topic的信息的时候nextVersion方法会被调用。
                                    nameServerDataVersion = DataVersion.decode(body, DataVersion.class);
                                    //如果当前broker的dataversion与nameserver的dataversion不相等，则表示需要继续宁更新
                                    if (!topicConfigWrapper.getDataVersion().equals(nameServerDataVersion)) {
                                        changed = true;
                                    }
                                }
                                if (changed == null || changed) {
                                    changedList.add(Boolean.TRUE);
                                }
                            }
                            default:
                                break;
                        }
                        log.warn("Query data version from name server {} OK,changed {}, broker {},name server {}", namesrvAddr, changed, topicConfigWrapper.getDataVersion(), nameServerDataVersion == null ? "" : nameServerDataVersion);
                    } catch (Exception e) {
                        changedList.add(Boolean.TRUE);
                        log.error("Query data version from name server {}  Exception, {}", namesrvAddr, e);
                    } finally {
                        countDownLatch.countDown();
                    }
                });

            }
            try {
                countDownLatch.await(timeoutMills, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                log.error("query dataversion from nameserver countDownLatch await Exception", e);
            }
        }
        return changedList;
    }

    public TopicConfigSerializeWrapper getAllTopicConfig(
        final String addr) throws RemotingConnectException, RemotingSendRequestException,
        RemotingTimeoutException, InterruptedException, MQBrokerException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ALL_TOPIC_CONFIG, null);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(true, addr), request, 3000);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return TopicConfigSerializeWrapper.decode(response.getBody(), TopicConfigSerializeWrapper.class);
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
    }

    public ConsumerOffsetSerializeWrapper getAllConsumerOffset(
        final String addr) throws InterruptedException, RemotingTimeoutException,
        RemotingSendRequestException, RemotingConnectException, MQBrokerException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ALL_CONSUMER_OFFSET, null);
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, 3000);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return ConsumerOffsetSerializeWrapper.decode(response.getBody(), ConsumerOffsetSerializeWrapper.class);
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
    }

    public String getAllDelayOffset(
        final String addr) throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException,
        RemotingConnectException, MQBrokerException, UnsupportedEncodingException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ALL_DELAY_OFFSET, null);
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, 3000);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return new String(response.getBody(), MixAll.DEFAULT_CHARSET);
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
    }

    public SubscriptionGroupWrapper getAllSubscriptionGroupConfig(
        final String addr) throws InterruptedException, RemotingTimeoutException,
        RemotingSendRequestException, RemotingConnectException, MQBrokerException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ALL_SUBSCRIPTIONGROUP_CONFIG, null);
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, 3000);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return SubscriptionGroupWrapper.decode(response.getBody(), SubscriptionGroupWrapper.class);
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
    }

    public void registerRPCHook(RPCHook rpcHook) {
        remotingClient.registerRPCHook(rpcHook);
    }
}
