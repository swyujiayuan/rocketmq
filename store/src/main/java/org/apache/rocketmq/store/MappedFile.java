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

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.store.CommitLog.PutMessageContext;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.util.LibC;
import sun.nio.ch.DirectBuffer;

public class MappedFile extends ReferenceResource {
    public static final int OS_PAGE_SIZE = 1024 * 4;
    protected static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private static final AtomicLong TOTAL_MAPPED_VIRTUAL_MEMORY = new AtomicLong(0);

    private static final AtomicInteger TOTAL_MAPPED_FILES = new AtomicInteger(0);
    protected final AtomicInteger wrotePosition = new AtomicInteger(0);
    protected final AtomicInteger committedPosition = new AtomicInteger(0);
    private final AtomicInteger flushedPosition = new AtomicInteger(0);
    protected int fileSize;
    protected FileChannel fileChannel;
    /**
     * Message will put to here first, and then reput to FileChannel if writeBuffer is not null.
     */
    protected ByteBuffer writeBuffer = null;
    protected TransientStorePool transientStorePool = null;
    private String fileName;
    private long fileFromOffset;
    private File file;
    private MappedByteBuffer mappedByteBuffer;
    private volatile long storeTimestamp = 0;
    private boolean firstCreateInQueue = false;

    public MappedFile() {
    }

    public MappedFile(final String fileName, final int fileSize) throws IOException {
        //调用init初始化
        init(fileName, fileSize);
    }

    public MappedFile(final String fileName, final int fileSize,
        final TransientStorePool transientStorePool) throws IOException {
        init(fileName, fileSize, transientStorePool);
    }

    public static void ensureDirOK(final String dirName) {
        if (dirName != null) {
            if (dirName.contains(MessageStoreConfig.MULTI_PATH_SPLITTER)) {
                String[] dirs = dirName.trim().split(MessageStoreConfig.MULTI_PATH_SPLITTER);
                for (String dir : dirs) {
                    createDirIfNotExist(dir);
                }
            } else {
                createDirIfNotExist(dirName);
            }
        }
    }

    private static void  createDirIfNotExist(String dirName) {
        File f = new File(dirName);
        if (!f.exists()) {
            boolean result = f.mkdirs();
            log.info(dirName + " mkdir " + (result ? "OK" : "Failed"));
        }
    }

    public static void clean(final ByteBuffer buffer) {
        if (buffer == null || !buffer.isDirect() || buffer.capacity() == 0)
            return;
        invoke(invoke(viewed(buffer), "cleaner"), "clean");
    }

    private static Object invoke(final Object target, final String methodName, final Class<?>... args) {
        return AccessController.doPrivileged((PrivilegedAction<Object>) () -> {
            try {
                Method method = method(target, methodName, args);
                method.setAccessible(true);
                return method.invoke(target);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        });
    }

    private static Method method(Object target, String methodName, Class<?>[] args)
        throws NoSuchMethodException {
        try {
            return target.getClass().getMethod(methodName, args);
        } catch (NoSuchMethodException e) {
            return target.getClass().getDeclaredMethod(methodName, args);
        }
    }

    private static ByteBuffer viewed(ByteBuffer buffer) {
        String methodName = "viewedBuffer";
        Method[] methods = buffer.getClass().getMethods();
        for (int i = 0; i < methods.length; i++) {
            if (methods[i].getName().equals("attachment")) {
                methodName = "attachment";
                break;
            }
        }

        ByteBuffer viewedBuffer = (ByteBuffer) invoke(buffer, methodName);
        if (viewedBuffer == null)
            return buffer;
        else
            return viewed(viewedBuffer);
    }

    public static int getTotalMappedFiles() {
        return TOTAL_MAPPED_FILES.get();
    }

    public static long getTotalMappedVirtualMemory() {
        return TOTAL_MAPPED_VIRTUAL_MEMORY.get();
    }

    public void init(final String fileName, final int fileSize,
        final TransientStorePool transientStorePool) throws IOException {
        //普通初始化
        init(fileName, fileSize);
        //设置写buffer，采用堆外内存
        this.writeBuffer = transientStorePool.borrowBuffer();
        this.transientStorePool = transientStorePool;
    }

    private void init(final String fileName, final int fileSize) throws IOException {
        //文件名。长度为20位，左边补零，剩余为起始偏移量，比如00000000000000000000代表了第一个文件，起始偏移量为0
        // 当第一个文件写满了，第二个文件为00000000001073741824，起始偏移量为1073741824，以此类推。消息顺序写入日志文件，效率很高，当文件满了，写入下一个文件
        this.fileName = fileName;
        //文件大小。默认1G=1073741824
        this.fileSize = fileSize;
        //构建file对象
        this.file = new File(fileName);
        //构建文件起始索引，就是取自文件名
        this.fileFromOffset = Long.parseLong(this.file.getName());
        boolean ok = false;

        //确保文件目录存在
        ensureDirOK(this.file.getParent());

        try {
            //对当前commitlog文件构建文件通道fileChannel
            this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel();
            //把commitlog文件完全的映射到虚拟内存，也就是内存映射，即mmap，提升读写性能
            this.mappedByteBuffer = this.fileChannel.map(MapMode.READ_WRITE, 0, fileSize);
            //记录数据
            TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(fileSize);
            TOTAL_MAPPED_FILES.incrementAndGet();
            ok = true;
        } catch (FileNotFoundException e) {
            log.error("Failed to create file " + this.fileName, e);
            throw e;
        } catch (IOException e) {
            log.error("Failed to map file " + this.fileName, e);
            throw e;
        } finally {
            if (!ok && this.fileChannel != null) {
                //释放fileChannel，注意释放fileChannel不会对之前的mappedByteBuffer映射产生影响
                this.fileChannel.close();
            }
        }
    }

    public long getLastModifiedTimestamp() {
        return this.file.lastModified();
    }

    public int getFileSize() {
        return fileSize;
    }

    public FileChannel getFileChannel() {
        return fileChannel;
    }

    /**
     * MappedFile的方法
     * <p>
     * 追加消息
     *
     * @param msg               消息
     * @param cb                回调函数
     * @param putMessageContext 存放消息上下文
     */
    public AppendMessageResult appendMessage(final MessageExtBrokerInner msg, final AppendMessageCallback cb,
            PutMessageContext putMessageContext) {
        //调用appendMessagesInner方法真正的进行消息存储。
        return appendMessagesInner(msg, cb, putMessageContext);
    }

    public AppendMessageResult appendMessages(final MessageExtBatch messageExtBatch, final AppendMessageCallback cb,
            PutMessageContext putMessageContext) {
        return appendMessagesInner(messageExtBatch, cb, putMessageContext);
    }

    /**
     * 首先获取当前文件的写指针，如果写指针小于文件的大小，那么就对消息进行追加处理。
     * 追加处理的是通过回调函数的doAppend方法执行的，分为单条消息，和批量消息的处理。
     * 最后会更新写指针的位置，以及存储时间。
     *
     * @param messageExt        消息
     * @param cb                回调函数
     * @param putMessageContext 存放消息上下文
     * @return
     */
    public AppendMessageResult appendMessagesInner(final MessageExt messageExt, final AppendMessageCallback cb,
            PutMessageContext putMessageContext) {
        assert messageExt != null;
        assert cb != null;

        //获取写入指针的位置
        int currentPos = this.wrotePosition.get();

        //如果小于文件大小，那么可以写入
        if (currentPos < this.fileSize) {
            //如果存在writeBuffer，即支持堆外缓存，那么则使用writeBuffer进行读写分离，否则使用mmap的方式写
            ByteBuffer byteBuffer = writeBuffer != null ? writeBuffer.slice() : this.mappedByteBuffer.slice();
            //设置写入位置
            byteBuffer.position(currentPos);
            AppendMessageResult result;
            /*
             * 通过回调函数执行实际写入
             */
            if (messageExt instanceof MessageExtBrokerInner) {
                //单条消息,真正的追加消息是通过AppendMessageCallback回调函数的doAppend方法执行的。
                // 这里回调函数的具体实现是DefaultAppendMessageCallback，它是位于CommitLog里面的一个内部类的实现。
                result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos,
                        (MessageExtBrokerInner) messageExt, putMessageContext);
            } else if (messageExt instanceof MessageExtBatch) {
                //批量消息
                result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos,
                        (MessageExtBatch) messageExt, putMessageContext);
            } else {
                return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
            }
            //更新写指针的位置
            this.wrotePosition.addAndGet(result.getWroteBytes());
            //更新存储实时间
            this.storeTimestamp = result.getStoreTimestamp();
            return result;
        }
        log.error("MappedFile.appendMessage return null, wrotePosition: {} fileSize: {}", currentPos, this.fileSize);
        return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
    }

    public long getFileFromOffset() {
        return this.fileFromOffset;
    }

    /**
     * 该方法用于将数据追加到MappedFile，
     * 这里仅仅是追加到对应的mappedByteBuffer中，基于mmap技术仅仅是将数据写入pageCache中，并没有立即刷盘，而是依靠操作系统判断刷盘，这样保证了写入的高性能。
     *
     * @param data
     * @return
     */
    public boolean appendMessage(final byte[] data) {
        //获取写入位置
        int currentPos = this.wrotePosition.get();

        //如果当前位置加上消息大小小于等于文件大小，那么将消息写入mappedByteBuffer
        if ((currentPos + data.length) <= this.fileSize) {
            try {
                //消息写入mappedByteBuffer即可，并没有执行刷盘
                ByteBuffer buf = this.mappedByteBuffer.slice();
                buf.position(currentPos);
                buf.put(data);
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            this.wrotePosition.addAndGet(data.length);
            return true;
        }

        return false;
    }

    /**
     * Content of data from offset to offset + length will be written to file.
     *
     * @param offset The offset of the subarray to be used.
     * @param length The length of the subarray to be used.
     */
    public boolean appendMessage(final byte[] data, final int offset, final int length) {
        int currentPos = this.wrotePosition.get();

        if ((currentPos + length) <= this.fileSize) {
            try {
                ByteBuffer buf = this.mappedByteBuffer.slice();
                buf.position(currentPos);
                buf.put(data, offset, length);
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            this.wrotePosition.addAndGet(length);
            return true;
        }

        return false;
    }

    /**
     * 该方法是需要执行刷盘的MappedFile实例调用的方法，用于完成刷盘操作。无论是同步还是异步刷盘，最终都是调用该方法。
     *
     * 大概步骤为：
     * 1. 判断是否可以刷盘。如果文件已经满了，或者如果flushLeastPages大于0，且脏页数量大于等于flushLeastPages，
     *      或者如果flushLeastPages等于0并且存在脏数据，这几种情况都会刷盘。
     * 2. 如果可以刷盘。那么增加引用次数，并且进行刷盘操作，如果使用了堆外内存，
     *      那么通过fileChannel#force强制刷盘，这是异步堆外内存走的逻辑。如果没有使用堆外内存，那么通过mappedByteBuffer#force强制刷盘，这是同步或者异步刷盘走的逻辑。
     * 3. 最后更新刷盘位置为写入位置，并返回。

     */
    public int flush(final int flushLeastPages) {
        //判断是否可以刷盘
        //如果文件已经满了，或者如果flushLeastPages大于0，且脏页数量大于等于flushLeastPages
        //或者如果flushLeastPages等于0并且存在脏数据，这几种情况都会刷盘
        if (this.isAbleToFlush(flushLeastPages)) {
            //增加对该MappedFile的引用次数
            if (this.hold()) {
                //获取写入位置
                int value = getReadPosition();

                try {
                    /*
                     * 只将数据追加到fileChannel或mappedByteBuffer中，不会同时追加到这两个里面。
                     */
                    //We only append data to fileChannel or mappedByteBuffer, never both.
                    //如果使用了堆外内存，那么通过fileChannel强制刷盘，这是异步堆外内存走的逻辑
                    if (writeBuffer != null || this.fileChannel.position() != 0) {
                        this.fileChannel.force(false);
                    } else {
                        //如果没有使用堆外内存，那么通过mappedByteBuffer强制刷盘，这是同步或者异步刷盘走的逻辑
                        this.mappedByteBuffer.force();
                    }
                } catch (Throwable e) {
                    log.error("Error occurred when force data to disk.", e);
                }

                //设置刷盘位置为写入位置
                this.flushedPosition.set(value);
                //减少对该MappedFile的引用次数
                this.release();
            } else {
                log.warn("in flush, hold failed, flush offset = " + this.flushedPosition.get());
                this.flushedPosition.set(getReadPosition());
            }
        }
        //获取最新刷盘位置
        return this.getFlushedPosition();
    }

    /**
     * 该方法是需要执行提交的MappedFile实例调用的方法，用于完成提交操作。
     *
     * 通过isAbleToCommit方法判断是否支持提交，其判断逻辑和isAbleToFlush方法一致。
     * 如果支持提交，那么调用commit0方法将堆外内存中的全部脏数据提交到filechannel。
     *
     * 如果所有的脏数据被提交到了FileChannel，那么归还堆外缓存，将堆外缓存重置，
     * 并存入内存池availableBuffers的头部，随后将writeBuffer置为null，下次再重新获取新的writeBuffer。
     *
     * @param commitLeastPages
     * @return
     */
    public int commit(final int commitLeastPages) {
        //如果堆外缓存为null，那么不需要提交数据到filechannel，所以只需将wrotePosition视为committedPosition返回即可。
        if (writeBuffer == null) {
            //no need to commit data to file channel, so just regard wrotePosition as committedPosition.
            return this.wrotePosition.get();
        }
        //是否支持提交，其判断逻辑和isAbleToFlush方法一致
        if (this.isAbleToCommit(commitLeastPages)) {
            //增加对该MappedFile的引用次数
            if (this.hold()) {
                //将堆外内存中的全部脏数据提交到filechannel
                commit0();
                this.release();
            } else {
                log.warn("in commit, hold failed, commit offset = " + this.committedPosition.get());
            }
        }

        // All dirty data has been committed to FileChannel.
        //所有的脏数据被提交到了FileChannel，那么归还堆外缓存
        if (writeBuffer != null && this.transientStorePool != null && this.fileSize == this.committedPosition.get()) {
            //将堆外缓存重置，并存入内存池availableBuffers的头部
            this.transientStorePool.returnBuffer(writeBuffer);
            //writeBuffer职位null，下次再重新获取
            this.writeBuffer = null;
        }

        //返回提交位置
        return this.committedPosition.get();
    }

    protected void commit0() {
        int writePos = this.wrotePosition.get();
        int lastCommittedPosition = this.committedPosition.get();

        if (writePos - lastCommittedPosition > 0) {
            try {
                ByteBuffer byteBuffer = writeBuffer.slice();
                byteBuffer.position(lastCommittedPosition);
                byteBuffer.limit(writePos);
                this.fileChannel.position(lastCommittedPosition);
                this.fileChannel.write(byteBuffer);
                this.committedPosition.set(writePos);
            } catch (Throwable e) {
                log.error("Error occurred when commit data to FileChannel.", e);
            }
        }
    }

    /**
     * 该方法用于判断当前是否可刷盘，大概流程为：
     *
     * 1. 首先获取刷盘位置flush和写入位置write。然后判断如果文件满了，即写入位置等于文件大小，那么直接返回true。
     * 2. 如果至少刷盘的页数flushLeastPages大于0，则需要比较写入位置与刷盘位置的差值，当差值大于等于指定的最少页数才能刷盘，这样可以防止频繁的刷盘。
     * 3. 否则，表示flushLeastPages为0，那么只要写入位置大于刷盘位置，即存在脏数据，那么就一定会刷盘。
     *
     * @param flushLeastPages
     * @return
     */
    private boolean isAbleToFlush(final int flushLeastPages) {
        //获取刷盘位置
        int flush = this.flushedPosition.get();
        //获取写入位置
        int write = getReadPosition();

        //如果文件已经满了，那么返回true
        if (this.isFull()) {
            return true;
        }

        //如果至少刷盘的页数大于0，则需要比较写入位置与刷盘位置的差值
        //当差值大于等于指定的页数才能刷盘，防止频繁的刷盘
        if (flushLeastPages > 0) {
            return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= flushLeastPages;
        }

        //否则，表示flushLeastPages为0，那么只要写入位置大于刷盘位置，即存在脏数据，那么就会刷盘
        return write > flush;
    }

    protected boolean isAbleToCommit(final int commitLeastPages) {
        int commit = this.committedPosition.get();
        int write = this.wrotePosition.get();

        if (this.isFull()) {
            return true;
        }

        if (commitLeastPages > 0) {
            return ((write / OS_PAGE_SIZE) - (commit / OS_PAGE_SIZE)) >= commitLeastPages;
        }

        return write > commit;
    }

    public int getFlushedPosition() {
        return flushedPosition.get();
    }

    public void setFlushedPosition(int pos) {
        this.flushedPosition.set(pos);
    }

    public boolean isFull() {
        return this.fileSize == this.wrotePosition.get();
    }

    public SelectMappedBufferResult selectMappedBuffer(int pos, int size) {
        //获取写入位置，即最大偏移量
        int readPosition = getReadPosition();
        //如果指定相对偏移量+截取大小 小于最大偏移量，那么截取内存
        if ((pos + size) <= readPosition) {
            if (this.hold()) {
                //从mappedByteBuffer截取一段内存
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            } else {
                log.warn("matched, but hold failed, request pos: " + pos + ", fileFromOffset: "
                    + this.fileFromOffset);
            }
        } else {
            log.warn("selectMappedBuffer request pos invalid, request pos: " + pos + ", size: " + size
                + ", fileFromOffset: " + this.fileFromOffset);
        }

        return null;
    }

    /**
     * 从指定相对偏移量开始从指定MappedFile中的mappedByteBuffer中截取一段ByteBuffer，
     * 这段内存存储着将要重放的消息。这段ByteBuffer和原mappedByteBuffer共享同一块内存，但是拥有自己的指针。
     *
     * 然后根据起始物理索引、截取的ByteBuffer、截取的ByteBuffer大小以及当前CommitLog对象构建一个SelectMappedBufferResult对象返回。
     *
     * @param pos
     * @return
     */
    public SelectMappedBufferResult selectMappedBuffer(int pos) {
        //获取写入位置，即最大偏移量
        int readPosition = getReadPosition();
        //如果指定相对偏移量小于最大偏移量并且大于等于0，那么截取内存
        if (pos < readPosition && pos >= 0) {
            if (this.hold()) {
                //从mappedByteBuffer截取一段内存
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                int size = readPosition - pos;
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                //根据起始物理索引、新的ByteBuffer、ByteBuffer大小、当前CommitLog对象构建一个SelectMappedBufferResult对象返回
                return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            }
        }

        return null;
    }

    @Override
    public boolean cleanup(final long currentRef) {
        if (this.isAvailable()) {
            log.error("this file[REF:" + currentRef + "] " + this.fileName
                + " have not shutdown, stop unmapping.");
            return false;
        }

        if (this.isCleanupOver()) {
            log.error("this file[REF:" + currentRef + "] " + this.fileName
                + " have cleanup, do not do it again.");
            return true;
        }

        clean(this.mappedByteBuffer);
        TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(this.fileSize * (-1));
        TOTAL_MAPPED_FILES.decrementAndGet();
        log.info("unmap file[REF:" + currentRef + "] " + this.fileName + " OK");
        return true;
    }

    public boolean destroy(final long intervalForcibly) {
        this.shutdown(intervalForcibly);

        if (this.isCleanupOver()) {
            try {
                this.fileChannel.close();
                log.info("close file channel " + this.fileName + " OK");

                long beginTime = System.currentTimeMillis();
                boolean result = this.file.delete();
                log.info("delete file[REF:" + this.getRefCount() + "] " + this.fileName
                    + (result ? " OK, " : " Failed, ") + "W:" + this.getWrotePosition() + " M:"
                    + this.getFlushedPosition() + ", "
                    + UtilAll.computeElapsedTimeMilliseconds(beginTime));
            } catch (Exception e) {
                log.warn("close file channel " + this.fileName + " Failed. ", e);
            }

            return true;
        } else {
            log.warn("destroy mapped file[REF:" + this.getRefCount() + "] " + this.fileName
                + " Failed. cleanupOver: " + this.cleanupOver);
        }

        return false;
    }

    public int getWrotePosition() {
        return wrotePosition.get();
    }

    public void setWrotePosition(int pos) {
        this.wrotePosition.set(pos);
    }

    /**
     * @return The max position which have valid data
     */
    public int getReadPosition() {
        return this.writeBuffer == null ? this.wrotePosition.get() : this.committedPosition.get();
    }

    public void setCommittedPosition(int pos) {
        this.committedPosition.set(pos);
    }

    /**
     * MappedFile的方法
     *
     * 建立了进程虚拟地址空间映射之后，并没有分配虚拟内存对应的物理内存，这里进行内存预热
     *
     * @param type  消息刷盘类型，默认 FlushDiskType.ASYNC_FLUSH;
     * @param pages 一页大小，默认4k
     */
    public void warmMappedFile(FlushDiskType type, int pages) {
        long beginTime = System.currentTimeMillis();
        // 创建一个新的字节缓冲区
        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        int flush = 0;
        long time = System.currentTimeMillis();
        //每隔4k大小写入一个0
        for (int i = 0, j = 0; i < this.fileSize; i += MappedFile.OS_PAGE_SIZE, j++) {
            //每隔4k大小写入一个0
            byteBuffer.put(i, (byte) 0);
            // force flush when flush disk type is sync
            //如果是同步刷盘，则每次写入都要强制刷盘
            if (type == FlushDiskType.SYNC_FLUSH) {
                if ((i / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE) >= pages) {
                    flush = i;
                    mappedByteBuffer.force();
                }
            }

            // prevent gc
            //调用Thread.sleep(0)当前线程主动放弃CPU资源，立即进入就绪状态
            //防止因为多次循环导致该线程一直抢占着CPU资源不释放，
            if (j % 1000 == 0) {
                log.info("j={}, costTime={}", j, System.currentTimeMillis() - time);
                time = System.currentTimeMillis();
                try {
                    Thread.sleep(0);
                } catch (InterruptedException e) {
                    log.error("Interrupted", e);
                }
            }
        }

        // force flush when prepare load finished
        //把剩余的数据强制刷新到磁盘中
        if (type == FlushDiskType.SYNC_FLUSH) {
            log.info("mapped file warm-up done, force to disk, mappedFile={}, costTime={}",
                this.getFileName(), System.currentTimeMillis() - beginTime);
            mappedByteBuffer.force();
        }
        log.info("mapped file warm-up done. mappedFile={}, costTime={}", this.getFileName(),
            System.currentTimeMillis() - beginTime);

        /*
         * 当实现了文件内存预热之后，虽然短时间不会读取数据不会引发缺页异常，
         * 但是当内存不足的时候，一部分不常使用的内存还是会被交换到swap空间中，当程序再次读取交换出去的数据的时候会再次产生缺页异常。
         *
         * 因此RocketMQ在warmMappedFile方法的最后还调用了mlock方法，该方法调用系统mlock函数，锁定该文件的Page Cache，防止把预热过的文件被操作系统调到swap空间中。
         * 另外还会调用系统madvise函数，再次尝试一次性先将一段数据读入到映射内存区域，这样就减少了缺页异常的产生。
         */
        this.mlock();
    }

    public String getFileName() {
        return fileName;
    }

    public MappedByteBuffer getMappedByteBuffer() {
        return mappedByteBuffer;
    }

    public ByteBuffer sliceByteBuffer() {
        return this.mappedByteBuffer.slice();
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public boolean isFirstCreateInQueue() {
        return firstCreateInQueue;
    }

    public void setFirstCreateInQueue(boolean firstCreateInQueue) {
        this.firstCreateInQueue = firstCreateInQueue;
    }

    public void mlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        {
            //mlock调用
            int ret = LibC.INSTANCE.mlock(pointer, new NativeLong(this.fileSize));
            log.info("mlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }

        {
            //madvise调用
            int ret = LibC.INSTANCE.madvise(pointer, new NativeLong(this.fileSize), LibC.MADV_WILLNEED);
            log.info("madvise {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }
    }

    public void munlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        int ret = LibC.INSTANCE.munlock(pointer, new NativeLong(this.fileSize));
        log.info("munlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
    }

    //testable
    File getFile() {
        return this.file;
    }

    @Override
    public String toString() {
        return this.fileName;
    }
}
