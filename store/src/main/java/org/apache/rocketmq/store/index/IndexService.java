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
package org.apache.rocketmq.store.index;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.config.StorePathConfigHelper;

public class IndexService {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    /**
     * Maximum times to attempt index file creation.
     */
    private static final int MAX_TRY_IDX_CREATE = 3;
    private final DefaultMessageStore defaultMessageStore;
    private final int hashSlotNum;
    private final int indexNum;
    private final String storePath;
    private final ArrayList<IndexFile> indexFileList = new ArrayList<IndexFile>();
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    public IndexService(final DefaultMessageStore store) {
        this.defaultMessageStore = store;
        this.hashSlotNum = store.getMessageStoreConfig().getMaxHashSlotNum();
        this.indexNum = store.getMessageStoreConfig().getMaxIndexNum();
        this.storePath =
            StorePathConfigHelper.getStorePathIndex(store.getMessageStoreConfig().getStorePathRootDir());
    }

    /**
     * 加载 index 索引文件，目录路径为{storePathRootDir}/index。index 索引文件
     * 用于通过时间区间来快速查询消息，底层为HashMap结构，实现为hash索引。
     * 文件名fileName是以创建时的时间戳命名的，固定的单个IndexFile文件大小约为400M，
     * 一个IndexFile可以保存 2000W个索引，IndexFile的底层存储设计为在文件系统中实现HashMap结构，故rocketmq的索引文件其底层实现为hash索引。
     *
     * 最终一个index文件对应着一个IndexFile实例，并且会加到indexFileList集合中。
     * 还会判断如果上次broker不是正常退出，并且并且当前index文件中最后一个消息的落盘时间戳大于StoreCheckpoint中的最后一个index索引文件创建时间，则该索引文件被删除。
     *
     * @param lastExitOK
     * @return
     */
    public boolean load(final boolean lastExitOK) {
        //获取上级目录路径，{storePathRootDir}/index
        File dir = new File(this.storePath);
        //获取内部的index索引文件
        File[] files = dir.listFiles();
        if (files != null) {
            // ascending order
            // 按照文件名字中的时间戳排序
            Arrays.sort(files);
            for (File file : files) {
                try {
                    //一个index文件对应着一个IndexFile实例
                    IndexFile f = new IndexFile(file.getPath(), this.hashSlotNum, this.indexNum, 0, 0);
                    //加载index文件
                    f.load();

                    //如果上一次是异常推出，并且当前index文件中最后一个消息的落盘时间戳大于最后一个index索引文件创建时间，则该索引文件被删除
                    if (!lastExitOK) {
                        if (f.getEndTimestamp() > this.defaultMessageStore.getStoreCheckpoint()
                            .getIndexMsgTimestamp()) {
                            f.destroy(0);
                            continue;
                        }
                    }

                    log.info("load index file OK, " + f.getFileName());
                    //加入到索引文件集合
                    this.indexFileList.add(f);
                } catch (IOException e) {
                    log.error("load file {} error", file, e);
                    return false;
                } catch (NumberFormatException e) {
                    log.error("load file {} error", file, e);
                }
            }
        }

        return true;
    }

    public void deleteExpiredFile(long offset) {
        Object[] files = null;
        try {
            this.readWriteLock.readLock().lock();
            if (this.indexFileList.isEmpty()) {
                return;
            }

            long endPhyOffset = this.indexFileList.get(0).getEndPhyOffset();
            if (endPhyOffset < offset) {
                files = this.indexFileList.toArray();
            }
        } catch (Exception e) {
            log.error("destroy exception", e);
        } finally {
            this.readWriteLock.readLock().unlock();
        }

        if (files != null) {
            List<IndexFile> fileList = new ArrayList<IndexFile>();
            for (int i = 0; i < (files.length - 1); i++) {
                IndexFile f = (IndexFile) files[i];
                if (f.getEndPhyOffset() < offset) {
                    fileList.add(f);
                } else {
                    break;
                }
            }

            this.deleteExpiredFile(fileList);
        }
    }

    private void deleteExpiredFile(List<IndexFile> files) {
        if (!files.isEmpty()) {
            try {
                this.readWriteLock.writeLock().lock();
                for (IndexFile file : files) {
                    boolean destroyed = file.destroy(3000);
                    destroyed = destroyed && this.indexFileList.remove(file);
                    if (!destroyed) {
                        log.error("deleteExpiredFile remove failed.");
                        break;
                    }
                }
            } catch (Exception e) {
                log.error("deleteExpiredFile has exception.", e);
            } finally {
                this.readWriteLock.writeLock().unlock();
            }
        }
    }

    public void destroy() {
        try {
            this.readWriteLock.writeLock().lock();
            for (IndexFile f : this.indexFileList) {
                f.destroy(1000 * 3);
            }
            this.indexFileList.clear();
        } catch (Exception e) {
            log.error("destroy exception", e);
        } finally {
            this.readWriteLock.writeLock().unlock();
        }
    }

    public QueryOffsetResult queryOffset(String topic, String key, int maxNum, long begin, long end) {
        List<Long> phyOffsets = new ArrayList<Long>(maxNum);

        long indexLastUpdateTimestamp = 0;
        long indexLastUpdatePhyoffset = 0;
        maxNum = Math.min(maxNum, this.defaultMessageStore.getMessageStoreConfig().getMaxMsgsNumBatch());
        try {
            this.readWriteLock.readLock().lock();
            if (!this.indexFileList.isEmpty()) {
                for (int i = this.indexFileList.size(); i > 0; i--) {
                    IndexFile f = this.indexFileList.get(i - 1);
                    boolean lastFile = i == this.indexFileList.size();
                    if (lastFile) {
                        indexLastUpdateTimestamp = f.getEndTimestamp();
                        indexLastUpdatePhyoffset = f.getEndPhyOffset();
                    }

                    if (f.isTimeMatched(begin, end)) {

                        f.selectPhyOffset(phyOffsets, buildKey(topic, key), maxNum, begin, end);
                    }

                    if (f.getBeginTimestamp() < begin) {
                        break;
                    }

                    if (phyOffsets.size() >= maxNum) {
                        break;
                    }
                }
            }
        } catch (Exception e) {
            log.error("queryMsg exception", e);
        } finally {
            this.readWriteLock.readLock().unlock();
        }

        return new QueryOffsetResult(phyOffsets, indexLastUpdateTimestamp, indexLastUpdatePhyoffset);
    }

    /**
     * IndexService的方法
     * 构建key
     */
    private String buildKey(final String topic, final String key) {
        //拼接
        return topic + "#" + key;
    }

    /**
     * 该方法用于为一条消息构建Index索引，大概步骤为：
     *
     * 1. 通过retryGetAndCreateIndexFile方法获取或创建最新索引文件IndexFile，支持重试最多3次。
     * 2. 判断当前消息在commitlog中的偏移量小于该文件的结束索引在commitlog中的偏移量，那么表示已为该消息构建Index索引，直接返回。
     *      如果该消息是事务回滚消息，则同样直接返回，不需要创建索引。
     * 3. 获取客户端生成的uniqId，也被称为msgId，从逻辑上代表客户端生成的唯一一条消息，如果uniqId不为null，那么调用putKey方法为uniqId构建索引。
     * 4. 获取客户端传递的keys，如果keys不为空，那么调用putKey方法为keys中的每一个key构建索引。
     *
     * @param req
     */
    public void buildIndex(DispatchRequest req) {
        /*
         * 获取或创建最新索引文件，支持重试最多3次
         */
        IndexFile indexFile = retryGetAndCreateIndexFile();
        if (indexFile != null) {
            //获取结束物理索引
            long endPhyOffset = indexFile.getEndPhyOffset();
            DispatchRequest msg = req;
            //获取topic和keys
            String topic = msg.getTopic();
            String keys = msg.getKeys();
            //如果消息在commitlog中的偏移量小于该文件的结束索引在commitlog中的偏移量，那么表示已为该消息之后的消息构建Index索引
            //此时直接返回，不需要创建索引
            if (msg.getCommitLogOffset() < endPhyOffset) {
                return;
            }

            //获取该消息的事务类型
            final int tranType = MessageSysFlag.getTransactionValue(msg.getSysFlag());
            switch (tranType) {
                case MessageSysFlag.TRANSACTION_NOT_TYPE:
                case MessageSysFlag.TRANSACTION_PREPARED_TYPE:
                case MessageSysFlag.TRANSACTION_COMMIT_TYPE:
                    break;
                //如果是事务回滚消息，则直接返回，不需要创建索引
                case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
                    return;
            }

            //获取客户端生成的uniqId，也被称为msgId，从逻辑上代表客户端生成的唯一一条消息
            //如果uniqId不为null，那么为uniqId构建索引
            if (req.getUniqKey() != null) {
                indexFile = putKey(indexFile, msg, buildKey(topic, req.getUniqKey()));
                if (indexFile == null) {
                    log.error("putKey error commitlog {} uniqkey {}", req.getCommitLogOffset(), req.getUniqKey());
                    return;
                }
            }

            //获取客户端传递的keys
            //如果keys不为空，那么为keys中的每一个key构建索引
            if (keys != null && keys.length() > 0) {
                //按照空格拆分key
                String[] keyset = keys.split(MessageConst.KEY_SEPARATOR);
                //为keys中的每一个key构建索引
                for (int i = 0; i < keyset.length; i++) {
                    String key = keyset[i];
                    if (key.length() > 0) {
                        indexFile = putKey(indexFile, msg, buildKey(topic, key));
                        if (indexFile == null) {
                            log.error("putKey error commitlog {} uniqkey {}", req.getCommitLogOffset(), req.getUniqKey());
                            return;
                        }
                    }
                }
            }
        } else {
            log.error("build index error, stop building index");
        }
    }

    /**
     * IndexFile文件的存储位置是：$HOME\store\index${fileName}，文件名fileName是以创建时的时间戳命名的，
     * 文件大小是固定的，等于40+500W*4+2000W*20= 420000040个字节大小。
     * 即一个IndexFile可以保存2000W个索引，IndexFile的底层存储设计为在文件系统中实现HashMap结构，故rocketmq的索引文件其底层实现为hash索引。
     *
     * putKey方法就是构建Index索引的入口方法，该方法将会循环调用indexFile#putKey方法构建Index索引，
     * 而每次构建失败都将调用retryGetAndCreateIndexFile方法尝试获取或创建最新索引文件然后再尝试构建。
     *
     * @param indexFile
     * @param msg
     * @param idxKey
     * @return
     */
    private IndexFile putKey(IndexFile indexFile, DispatchRequest msg, String idxKey) {
        //循环尝试构建Index索引
        for (boolean ok = indexFile.putKey(idxKey, msg.getCommitLogOffset(), msg.getStoreTimestamp()); !ok; ) {
            log.warn("Index file [" + indexFile.getFileName() + "] is full, trying to create another one");

            //构建失败，则尝试获取或创建最新索引文件，支持重试
            indexFile = retryGetAndCreateIndexFile();
            if (null == indexFile) {
                return null;
            }

            //再次尝试构建Index索引
            ok = indexFile.putKey(idxKey, msg.getCommitLogOffset(), msg.getStoreTimestamp());
        }

        return indexFile;
    }

    /**
     * Retries to get or create index file.
     *
     * 该方法用于获取或创建索引文件，支持重试。方法中开启了一个循环，最多循环三次，
     * 在循环中调用getAndCreateLastIndexFile方法获取最新的索引文件，
     * 如果文件写满了或者还没有文件则会自动创建新的索引文件。
     *
     * @return {@link IndexFile} or null on failure.
     */
    public IndexFile retryGetAndCreateIndexFile() {
        IndexFile indexFile = null;

        //循环尝试，尝试创建索引文件的最大次数为3
        for (int times = 0; null == indexFile && times < MAX_TRY_IDX_CREATE; times++) {
            //获取最新的索引文件，如果文件写满了或者还没有文件则会自动创建新的索引文件
            indexFile = this.getAndCreateLastIndexFile();
            //如果获取的indexFile不为null，那么退出循环
            if (null != indexFile)
                break;

            try {
                log.info("Tried to create index file " + times + " times");
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                log.error("Interrupted", e);
            }
        }

        //标记indexFile异常
        if (null == indexFile) {
            this.defaultMessageStore.getAccessRights().makeIndexFileError();
            log.error("Mark index file cannot build flag");
        }

        return indexFile;
    }

    /**
     * 该方法尝试获取最新的索引文件，如果文件写满了或者还没有文件则会自动创建新的索引文件。大概步骤为：
     *
     * 1. 首先获取读锁。
     *      1.1 如果indexFileList不为空，那么尝试获取最后一个IndexFile，否则创建一个新的，比如第一次写。
     *      1.2 如果最后一个IndexFile没写满，则赋值给indexFile，后面直接返回。
     *      1.3 如果最后一个IndexFile写满了，则创建新文件，获取目前最后一个文件的endPhyOffset，获取目前最后一个文件的endTimestamp等信息。
     *      1.4 释放读锁。
     * 2. 如果上一步没有获取到indexFile，那么尝试创建一个新的IndexFile。
     *      2.1 获取完整文件名$HOME/store/index${fileName}，fileName是以创建时的时间戳命名的，精确到毫秒，例如20220512214613292。
     *      2.2 调用IndexFile的构造器创建新的IndexFile。
     *      2.3 获取写锁。将新建的IndexFile加入到indexFileList集合尾部。释放写锁。
     *      2.4 创建了新的文件之后，那么尝试将上一个文件刷盘。新开一个线程，异步的调用IndexService#flush方法对上一个IndexFile文件刷盘。
     * 3. 最后返回获取的indexFile。
     *
     *
     * 这里尝试获取的是最新的IndexService，并且引入了读写锁的设计。
     * 在获取indexFileList的最后一个元素时使用读锁，而在创建了indexFile之后向indexFileList添加indexFile的时候使用写锁。
     * 使用读写锁的好处很明显，既保证了数据安全，同时保证了效率的最大化，因为Index文件的使用永远都是查询远远大于创建的。
     *
     * @return
     */
    public IndexFile getAndCreateLastIndexFile() {
        IndexFile indexFile = null;
        IndexFile prevIndexFile = null;
        long lastUpdateEndPhyOffset = 0;
        long lastUpdateIndexTimestamp = 0;
        /*
         * 尝试获取最新IndexFile
         */

        {
            //尝试获取读锁
            this.readWriteLock.readLock().lock();
            //如果indexFileList不为空
            if (!this.indexFileList.isEmpty()) {
                //尝试获取最后一个IndexFile
                IndexFile tmp = this.indexFileList.get(this.indexFileList.size() - 1);
                if (!tmp.isWriteFull()) {
                    //如果最后一个IndexFile没写满，则赋值给indexFile
                    indexFile = tmp;
                } else {
                    //如果最后一个IndexFile写满了，则创建新文件
                    //获取目前最后一个文件的endPhyOffset
                    lastUpdateEndPhyOffset = tmp.getEndPhyOffset();
                    //获取目前最后一个文件的endTimestamp
                    lastUpdateIndexTimestamp = tmp.getEndTimestamp();
                    //赋值给prevIndexFile
                    prevIndexFile = tmp;
                }
            }

            this.readWriteLock.readLock().unlock();
        }

        /*
         * 尝试创建一个新的IndexFile
         */
        if (indexFile == null) {
            try {
                //获取完整文件名$HOME/store/index${fileName}，fileName是以创建时的时间戳命名的，精确到毫秒
                String fileName =
                    this.storePath + File.separator
                        + UtilAll.timeMillisToHumanString(System.currentTimeMillis());
                /*
                创建IndexFile
                 * 当第一次构建Index或者之前的IndexFile写满了的时候，需要通过IndexFile的构造器创建新的IndexFile。
                 *
                 * Index文件的存储位置是：$HOME/store/index${fileName}，文件名fileName是以创建时的时间戳命名的，
                 * 固定的单个IndexFile文件大小约为：40B 头数据indexHeader + 500w * 4B hashslot + 2000w * 20B index = 420000040B，约为400M。
                 *
                 */
                indexFile =
                    new IndexFile(fileName, this.hashSlotNum, this.indexNum, lastUpdateEndPhyOffset,
                        lastUpdateIndexTimestamp);
                //获取写锁
                this.readWriteLock.writeLock().lock();
                //加入到indexFileList集合中
                this.indexFileList.add(indexFile);
            } catch (Exception e) {
                log.error("getLastIndexFile exception ", e);
            } finally {
                //释放写锁
                this.readWriteLock.writeLock().unlock();
            }

            /*
             * 创建了新的文件之后，尝试将上一个文件刷盘
             */
            if (indexFile != null) {
                final IndexFile flushThisFile = prevIndexFile;
                /*
                 * 新开一个线程，异步的对上一个IndexFile文件刷盘
                 */
                Thread flushThread = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        IndexService.this.flush(flushThisFile);
                    }
                }, "FlushIndexFileThread");

                flushThread.setDaemon(true);
                flushThread.start();
            }
        }

        return indexFile;
    }

    public void flush(final IndexFile f) {
        if (null == f)
            return;

        long indexMsgTimestamp = 0;

        if (f.isWriteFull()) {
            indexMsgTimestamp = f.getEndTimestamp();
        }

        f.flush();

        if (indexMsgTimestamp > 0) {
            this.defaultMessageStore.getStoreCheckpoint().setIndexMsgTimestamp(indexMsgTimestamp);
            this.defaultMessageStore.getStoreCheckpoint().flush();
        }
    }

    public void start() {

    }

    public void shutdown() {

    }
}
