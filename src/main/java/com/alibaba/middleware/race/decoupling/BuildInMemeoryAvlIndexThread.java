package com.alibaba.middleware.race.decoupling;

import com.alibaba.middleware.race.cache.LimitedAvlTree;
import com.alibaba.middleware.race.storage.Extent;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * Created by xiyuanbupt on 7/19/16.
 * 将DiskLocQueues 中的数据插入到内存中的avl 中
 * 内存avl 数据同步到磁盘由另外的线程负责
 */
abstract public class BuildInMemeoryAvlIndexThread<T extends Comparable<? super T>> implements Runnable {

    private static Logger LOG = Logger.getLogger(BuildInMemeoryAvlIndexThread.class.getName());

    private final LinkedBlockingDeque<T> keysQueue;

    /**
     * use to Judge all related thread finish copy
     */
    private final AtomicInteger nRemain;

    public BuildInMemeoryAvlIndexThread(LinkedBlockingDeque keysQueue,AtomicInteger nRemain){
        this.keysQueue = keysQueue;
        this.nRemain = nRemain;
    }

    @Override
    public void run() {
        while(true){
            try {
                T t = keysQueue.poll(30, TimeUnit.SECONDS);
                if(t==null){
                     // judge thread is finish
                    if(nRemain.get()==0){
                        LOG.info("finsish insert all index data to memory avl");
                        break;
                    }
                }
                insertKeys(t);
            }catch (Exception e){
                e.printStackTrace();
                System.exit(-1);
            }

        }
    }

    abstract protected void insertKeys(T t);
}
