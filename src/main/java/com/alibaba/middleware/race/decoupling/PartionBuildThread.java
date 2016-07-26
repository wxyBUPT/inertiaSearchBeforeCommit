package com.alibaba.middleware.race.decoupling;

import com.alibaba.middleware.race.RaceConf;
import com.alibaba.middleware.race.cache.LimitedAvlTree;
import com.alibaba.middleware.race.storage.Indexable;

import java.io.Serializable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * Created by xiyuanbupt on 7/26/16.
 * 与buildThrea类似,但是为添加了一层hash 之后将数据插入到 indexPartion 中的
 */
public class PartionBuildThread <T extends Comparable<? super T> & Serializable & Indexable> implements Runnable{

    protected static Logger LOG = Logger.getLogger(PartionBuildThread.class.getName());
    //用来缓存的有界队列
    protected LinkedBlockingDeque<T> keysQueue;
    /**
     * 用来判断原始数据复制线程是否完成数据复制
     */
    protected final AtomicInteger nRemain;

    /**
     * 为了读写互不阻塞,所以设置一个队列,队列里面只有两个元素,写完数据使用新的线程将数据flush 到磁盘,等线程执行完毕
     * 还回队列中的元素
     */
    LimitedAvlTree<T> inMemoryTree;

    protected int flushCount = 0;
    /**
     * 依旧使用flushUtil 做数据迁移,不过flushUtil 在七月26日之后需要重写
     */
    protected FlushUtil<T> flushUtil;
    protected CountDownLatch sendFinishSingle;

    public PartionBuildThread(final AtomicInteger nRemain,CountDownLatch sendFinishSingle){
        this.nRemain = nRemain;
        inMemoryTree = new LimitedAvlTree<>(RaceConf.INMEMORYMAXINDEXSIZE);
        this.sendFinishSingle = sendFinishSingle;
    }

    @Override
    public void run() {

    }
}
