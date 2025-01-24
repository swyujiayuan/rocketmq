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
package org.apache.rocketmq.store;

import java.io.File;
import java.io.IOException;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.config.BrokerRole;

/**
 * Create MappedFile in advance
 */
public class AllocateMappedFileService extends ServiceThread {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private static int waitTimeOut = 1000 * 5;
    private ConcurrentMap<String, AllocateRequest> requestTable =
        new ConcurrentHashMap<String, AllocateRequest>();
    private PriorityBlockingQueue<AllocateRequest> requestQueue =
        new PriorityBlockingQueue<AllocateRequest>();
    private volatile boolean hasException = false;
    private DefaultMessageStore messageStore;

    public AllocateMappedFileService(DefaultMessageStore messageStore) {
        this.messageStore = messageStore;
    }

    /**
     * MappedFile作为一个RocketMQ的物理文件在Java中的映射类。
     * 实际上，commitLog consumerQueue、indexFile3种文件磁盘的读写都是通过MappedFile操作的。
     * 它的构造器中会对当前文件进行mmap内存映射操作。
     *
     * putRequestAndReturnMappedFile方法用于创建MappedFile，会同时创建两个MappedFile，
     * 一个同步创建并返回用于本次实际使用，一个后台异步创建用于下次取用。
     * 这样的好处是避免等到当前文件真正用完了才创建下一个文件，目的同样是提升性能。
     *
     * 这里的同步和异步实际上都是通过一个服务线程执行的，
     * 该方法只是提交两个映射文件创建请求AllocateRequest，并且提交到requestTable和requestQueue中。
     * 随后当前线程只会同步等待第一个映射文件的创建，最多等待5s，如果创建成功则返回，而较大的offset那一个映射文件则会异步的创建，不会等待。
     *
     * 这里线程等待使用的是倒计数器CountDownLatch，
     * 一个请求一个AllocateRequest对象，其内部还持有一个CountDownLatch对象，
     * 当该请求对应的MappedFile被创建完毕之后，会调用内部的CountDownLatch#countDown方法，自然会唤醒该等待的线程。
     *
     * @param nextFilePath
     * @param nextNextFilePath
     * @param fileSize 文件大小默认1G
     * @return
     */
    public MappedFile putRequestAndReturnMappedFile(String nextFilePath, String nextNextFilePath, int fileSize) {
        //可以提交的请求
        int canSubmitRequests = 2;
        //如果当前节点不是从节点，并且是异步刷盘策略，并且transientStorePoolEnable参数配置为true，并且fastFailIfNoBufferInStorePool为true
        //那么重新计算最多可以提交几个文件创建请求
        if (this.messageStore.getMessageStoreConfig().isTransientStorePoolEnable()) {
            if (this.messageStore.getMessageStoreConfig().isFastFailIfNoBufferInStorePool()
                && BrokerRole.SLAVE != this.messageStore.getMessageStoreConfig().getBrokerRole()) { //if broker is slave, don't fast fail even no buffer in pool
                canSubmitRequests = this.messageStore.getTransientStorePool().availableBufferNums() - this.requestQueue.size();
            }
        }

        //根据nextFilePath创建一个请求对象，并将请求对象存入requestTable这个map集合中
        AllocateRequest nextReq = new AllocateRequest(nextFilePath, fileSize);
        boolean nextPutOK = this.requestTable.putIfAbsent(nextFilePath, nextReq) == null;

        //如果存入成功
        if (nextPutOK) {
            if (canSubmitRequests <= 0) {
                log.warn("[NOTIFYME]TransientStorePool is not enough, so create mapped file error, " +
                    "RequestQueueSize : {}, StorePoolSize: {}", this.requestQueue.size(), this.messageStore.getTransientStorePool().availableBufferNums());
                this.requestTable.remove(nextFilePath);
                return null;
            }
            //将请求存入requestQueue中
            boolean offerOK = this.requestQueue.offer(nextReq);
            if (!offerOK) {
                log.warn("never expected here, add a request to preallocate queue failed");
            }
            //可以提交的请求数量自减
            canSubmitRequests--;
        }

        //根据nextNextFilePath创建另一个请求对象，并将请求对象存入requestTable这个map集合中
        AllocateRequest nextNextReq = new AllocateRequest(nextNextFilePath, fileSize);
        boolean nextNextPutOK = this.requestTable.putIfAbsent(nextNextFilePath, nextNextReq) == null;
        if (nextNextPutOK) {
            if (canSubmitRequests <= 0) {
                log.warn("[NOTIFYME]TransientStorePool is not enough, so skip preallocate mapped file, " +
                    "RequestQueueSize : {}, StorePoolSize: {}", this.requestQueue.size(), this.messageStore.getTransientStorePool().availableBufferNums());
                this.requestTable.remove(nextNextFilePath);
            } else {
                //将请求存入requestQueue中
                boolean offerOK = this.requestQueue.offer(nextNextReq);
                if (!offerOK) {
                    log.warn("never expected here, add a request to preallocate queue failed");
                }
            }
        }

        //有异常就直接返回
        if (hasException) {
            log.warn(this.getServiceName() + " service has exception. so return null");
            return null;
        }

        //获取此前存入的nextFilePath对应的请求
        AllocateRequest result = this.requestTable.get(nextFilePath);
        try {
            if (result != null) {
                //同步等待最多5s
                boolean waitOK = result.getCountDownLatch().await(waitTimeOut, TimeUnit.MILLISECONDS);
                if (!waitOK) {
                    //超时
                    log.warn("create mmap timeout " + result.getFilePath() + " " + result.getFileSize());
                    return null;
                } else {
                    //如果nextFilePath对应的MappedFile创建成功，那么从requestTable移除对应的请求
                    this.requestTable.remove(nextFilePath);
                    //返回创建的mappedFile
                    return result.getMappedFile();
                }
            } else {
                log.error("find preallocate mmap failed, this never happen");
            }
        } catch (InterruptedException e) {
            log.warn(this.getServiceName() + " service has exception. ", e);
        }

        return null;
    }

    @Override
    public String getServiceName() {
        return AllocateMappedFileService.class.getSimpleName();
    }

    @Override
    public void shutdown() {
        super.shutdown(true);
        for (AllocateRequest req : this.requestTable.values()) {
            if (req.mappedFile != null) {
                log.info("delete pre allocated maped file, {}", req.mappedFile.getFileName());
                req.mappedFile.destroy(1000);
            }
        }
    }

    /**
     * AllocateMappedFileService的线程任务，内部是一个死循环，如果服务没有停止，并且没有被线程中断，那么一直循环执行mmapOperation方法。
     *
     * 创建mappedFile
     *
     */
    public void run() {
        log.info(this.getServiceName() + " service started");

        //死循环
        //如果服务没有停止，并且没有被线程中断，那么一直循环执行mmapOperation方法
        while (!this.isStopped() && this.mmapOperation()) {

        }
        log.info(this.getServiceName() + " service end");
    }

    /**
     * Only interrupted by the external thread, will return false
     *  mmap 操作，只有被外部线程中断，才会返回false
     * 该方法用于创建MappedFile。大概步骤为：
     *
     * 1. 从requestQueue中获取优先级最高的一个请求，即文件名最小或者说起始offset最小的请求。requestQueue是一个优先级队列。
     * 2. 判断是否需要通过堆外内存创建MappedFile，如果当前节点不是从节点，并且是异步刷盘策略，并且transientStorePoolEnable参数配置为true，那么使用堆外内存，默认不使用。
     *      2.1 RocketMQ中引入的 transientStorePoolEnable 能缓解 pagecache 的压力，其原理是基于DirectByteBuffer和MappedByteBuffer的读写分离。
     *      2.2 消息先写入DirectByteBuffer（堆外内存），随后从MappedByteBuffer（pageCache）读取。
     * 3. 如果没有启动堆外内存，那么普通方式创建mappedFile，并且进行mmap操作。
     * 4. 如果mappedFile大小大于等于1G并且warmMapedFileEnable参数为true，那么预写mappedFile，也就是所谓的内存预热或者文件预热。
     *      注意warmMapedFileEnable参数默认为false，即默认不开启文件预热，因此需要手动开启。
     * 5. 如果创建成功，那么将请求对象中的countDownLatch释放计数，这样就可以唤醒在putRequestAndReturnMappedFile方法中被阻塞的线程了。
     */
    private boolean mmapOperation() {
        boolean isSuccess = false;
        AllocateRequest req = null;
        try {
            //从requestQueue中获取优先级最高的一个请求，即文件名最小或者说起始offset最小的请求
            //requestQueue是一个优先级队列
            req = this.requestQueue.take();
            //从requestTable获取对应的请求
            AllocateRequest expectedRequest = this.requestTable.get(req.getFilePath());
            if (null == expectedRequest) {
                log.warn("this mmap request expired, maybe cause timeout " + req.getFilePath() + " "
                    + req.getFileSize());
                return true;
            }
            if (expectedRequest != req) {
                log.warn("never expected here,  maybe cause timeout " + req.getFilePath() + " "
                    + req.getFileSize() + ", req:" + req + ", expectedRequest:" + expectedRequest);
                return true;
            }

            //获取对应的mappedFile，如果为null则创建
            if (req.getMappedFile() == null) {
                //起始时间
                long beginTime = System.currentTimeMillis();

                MappedFile mappedFile;
                //如果当前节点不是从节点，并且是异步刷盘策略，并且transientStorePoolEnable参数配置为true，那么使用堆外内存，默认不使用
                //RocketMQ中引入的 transientStorePoolEnable 能缓解 pagecache 的压力，其原理是基于DirectByteBuffer和MappedByteBuffer的读写分离
                //消息先写入DirectByteBuffer（堆外内存），随后从MappedByteBuffer（pageCache）读取。
                if (messageStore.getMessageStoreConfig().isTransientStorePoolEnable()) {
                    try {
                        //可以基于SPI机制获取自定义的MappedFile
                        mappedFile = ServiceLoader.load(MappedFile.class).iterator().next();
                        //初始化
                        mappedFile.init(req.getFilePath(), req.getFileSize(), messageStore.getTransientStorePool());
                    } catch (RuntimeException e) {
                        log.warn("Use default implementation.");
                        mappedFile = new MappedFile(req.getFilePath(), req.getFileSize(), messageStore.getTransientStorePool());
                    }
                } else {
                    //普通方式创建mappedFile，该构造器内部将会以给定的文件路径创建一个file，并且把commitlog文从磁盘空间件完全的映射到虚拟内存，也就是内存映射，即mmap，提升读写性能。
                    mappedFile = new MappedFile(req.getFilePath(), req.getFileSize());
                }

                //创建mappedFile消耗的时间
                long elapsedTime = UtilAll.computeElapsedTimeMilliseconds(beginTime);
                if (elapsedTime > 10) {
                    int queueSize = this.requestQueue.size();
                    log.warn("create mappedFile spent time(ms) " + elapsedTime + " queue size " + queueSize
                        + " " + req.getFilePath() + " " + req.getFileSize());
                }

                // pre write mappedFile
                //如果mappedFile大小大于等于1G并且warmMapedFileEnable参数为true，那么预写mappedFile，也就是所谓的内存预热或者文件预热
                //注意warmMapedFileEnable参数默认为false，即默认不开启文件预热，因此选哟手动开启
                if (mappedFile.getFileSize() >= this.messageStore.getMessageStoreConfig()
                    .getMappedFileSizeCommitLog()
                    &&
                    this.messageStore.getMessageStoreConfig().isWarmMapedFileEnable()) {
                    /*
                     * 预热文件，
                     * mmap操作减少了传统IO将磁盘文件数据在操作系统内核地址空间的缓冲区和用户应用程序地址空间的缓冲区之间来回进行拷贝的性能开销，这是它的好处。
                     * 但是mmap操作对于OS来说只是建立虚拟内存地址至物理地址的映射关系，即将进程使用的虚拟内存地址映射到物理地址上。
                     * 实际上并不会加载任何MappedFile数据至内存中，也并不会分配指定的大小的内存。当
                     * 程序要访问数据时，如果发现这部分数据页并没有实际加载到内存中，则处理器自动触发一个缺页异常，进而进入内核空间再分配物理内存，一
                     * 次分配大小默认4k。一个G大小的CommitLog，如果靠着缺页中断来分配实际内存，那么需要触发26w多次缺页中断，这是一笔不小的开销。
                     * RocketMQ避免频繁发生却也异常的做法是采用文件预热，即让操作系统提前分配物理内存空间，防止在写入消息时发生缺页异常才进行分配。
                     *
                     */
                    mappedFile.warmMappedFile(this.messageStore.getMessageStoreConfig().getFlushDiskType(),
                        this.messageStore.getMessageStoreConfig().getFlushLeastPagesWhenWarmMapedFile());
                }

                req.setMappedFile(mappedFile);
                this.hasException = false;
                isSuccess = true;
            }
        } catch (InterruptedException e) {
            log.warn(this.getServiceName() + " interrupted, possibly by shutdown.");
            this.hasException = true;
            return false;
        } catch (IOException e) {
            log.warn(this.getServiceName() + " service has exception. ", e);
            this.hasException = true;
            if (null != req) {
                requestQueue.offer(req);
                try {
                    Thread.sleep(1);
                } catch (InterruptedException ignored) {
                }
            }
        } finally {
            if (req != null && isSuccess)
                //如果创建成功，那么将请求对象中的countDownLatch释放计数，这样就可以唤醒在putRequestAndReturnMappedFile方法中被阻塞的线程了
                req.getCountDownLatch().countDown();
        }
        return true;
    }

    static class AllocateRequest implements Comparable<AllocateRequest> {
        // Full file path
        private String filePath;
        private int fileSize;
        private CountDownLatch countDownLatch = new CountDownLatch(1);
        private volatile MappedFile mappedFile = null;

        public AllocateRequest(String filePath, int fileSize) {
            this.filePath = filePath;
            this.fileSize = fileSize;
        }

        public String getFilePath() {
            return filePath;
        }

        public void setFilePath(String filePath) {
            this.filePath = filePath;
        }

        public int getFileSize() {
            return fileSize;
        }

        public void setFileSize(int fileSize) {
            this.fileSize = fileSize;
        }

        public CountDownLatch getCountDownLatch() {
            return countDownLatch;
        }

        public void setCountDownLatch(CountDownLatch countDownLatch) {
            this.countDownLatch = countDownLatch;
        }

        public MappedFile getMappedFile() {
            return mappedFile;
        }

        public void setMappedFile(MappedFile mappedFile) {
            this.mappedFile = mappedFile;
        }

        public int compareTo(AllocateRequest other) {
            if (this.fileSize < other.fileSize)
                return 1;
            else if (this.fileSize > other.fileSize) {
                return -1;
            } else {
                int mIndex = this.filePath.lastIndexOf(File.separator);
                long mName = Long.parseLong(this.filePath.substring(mIndex + 1));
                int oIndex = other.filePath.lastIndexOf(File.separator);
                long oName = Long.parseLong(other.filePath.substring(oIndex + 1));
                if (mName < oName) {
                    return -1;
                } else if (mName > oName) {
                    return 1;
                } else {
                    return 0;
                }
            }
            // return this.fileSize < other.fileSize ? 1 : this.fileSize >
            // other.fileSize ? -1 : 0;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((filePath == null) ? 0 : filePath.hashCode());
            result = prime * result + fileSize;
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            AllocateRequest other = (AllocateRequest) obj;
            if (filePath == null) {
                if (other.filePath != null)
                    return false;
            } else if (!filePath.equals(other.filePath))
                return false;
            if (fileSize != other.fileSize)
                return false;
            return true;
        }
    }
}
