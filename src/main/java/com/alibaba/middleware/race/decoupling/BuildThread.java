package com.alibaba.middleware.race.decoupling;

import com.alibaba.middleware.race.RaceConf;
import com.alibaba.middleware.race.cache.LimitedAvlTree;
import com.alibaba.middleware.race.storage.DiskLoc;

import java.util.LinkedList;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * Created by xiyuanbupt on 7/20/16.
 */
public abstract class BuildThread<T extends Comparable<? super T>> implements Runnable{
    protected static Logger LOG = Logger.getLogger(BuildThread.class.getName());
    protected LinkedBlockingDeque<T> keysQueue;

    /**
     * 用来判断原始数据复制线程是否完成数据复制
     */
    protected final AtomicInteger nRemain;
    /**
     * 保存所有index leaf node 的位置信息,因为索引为一次性创建,所以先创建链表,之后创建索引
     */
    protected LinkedList<DiskLoc> sortedKeysInDisk;
    /**
     * 一个有限的avlTree,实现思想类似于lsm tree,为lsm tree 的 C0
     */
    protected LimitedAvlTree<T> inMemoryTree;
    /**
     * 记录向磁盘中同步了多少次数据
     */
    protected int flushCount = 0;
    protected FlushUtil<T> flushUtil;

    /**
     * bTreeRoot 的引用
     */

    public BuildThread(AtomicInteger nRemain){
        this.nRemain = nRemain;
        this.inMemoryTree = new LimitedAvlTree<>(RaceConf.INMEMORYMAXINDEXSIZE);
        this.sortedKeysInDisk = new LinkedList<>();
        this.flushUtil = new FlushUtil<>();
    }

    @Override
    public void run() {
        while(true){
            try{
                T keys = keysQueue.poll(30, TimeUnit.SECONDS);
                if(keys == null){
                    if(nRemain.get()==0){
                        LOG.info("finsh insert all index data of "  );
                        break;
                    }
                }
                insertKeys(keys);
            }catch (Exception e){
                e.printStackTrace();
                System.exit(-1);
            }
        }
        LOG.info("finsh create in memory index, flush all inmemory Tree to disk , total flush count is " + ++flushCount);
        sortedKeysInDisk = flushUtil.flushAvlToDisk(inMemoryTree,sortedKeysInDisk);
        createBPlusTree();
    }

    private void insertKeys(T key){
        if(inMemoryTree.isFull()){
            LOG.info("inmemory Tree is full , flush inmemory tree to disk, flush count is " + ++flushCount);
            sortedKeysInDisk = flushUtil.flushAvlToDisk(inMemoryTree,sortedKeysInDisk);
        }
        inMemoryTree.insert(key);
    }

    protected abstract void createBPlusTree();
}
