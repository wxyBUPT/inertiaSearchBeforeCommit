package com.alibaba.middleware.race.decoupling;

import com.alibaba.middleware.race.RaceConf;
import com.alibaba.middleware.race.cache.LimitedAvlTree;
import com.alibaba.middleware.race.cache.LimitedBinarySearchTree;
import com.alibaba.middleware.race.storage.*;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * Created by xiyuanbupt on 7/20/16.
 */
public abstract class BuildThread<T extends Comparable<? super T> & Serializable & Indexable> implements Runnable{
    protected static Logger LOG = Logger.getLogger(BuildThread.class.getName());
    //缓存索引的有界队列
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
    protected LimitedBinarySearchTree<T> inMemoryTree;
    /**
     * 记录向磁盘中同步了多少次数据
     */
    protected int flushCount = 0;
    protected FlushUtil<T> flushUtil;
    protected CountDownLatch sendFinishSingle;

    /**
     * bTreeRoot 的引用
     */

    public BuildThread(final AtomicInteger nRemain, CountDownLatch sendFinishSingle){
        this.nRemain = nRemain;
        this.inMemoryTree = new LimitedBinarySearchTree<>(RaceConf.INMEMORYMAXINDEXSIZE);
        this.sortedKeysInDisk = new LinkedList<>();
        this.flushUtil = new FlushUtil<>();
        this.sendFinishSingle = sendFinishSingle;
        /**
         * Use to report condition
         * for debug
         */
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(1000);
                }catch (Exception e){

                }
                while(true){
                    LOG.info(inMemoryTree.getInfo());
                    try{
                        Thread.sleep(20000);
                    }catch (Exception e){

                    }
                }
            }
        }).start();
    }

    @Override
    public void run() {
        LOG.info("Thread start ");
        while(true){
            try{
                T keys = keysQueue.poll(1, TimeUnit.SECONDS);
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
        cacheRoot();
        sendFinishSingle.countDown();
        LOG.info("finsh all");
        //System.out.println("This is for test !!!!!!!!!!");
        //forTest();
    }

    private void insertKeys(T key){
        if(inMemoryTree.isFull()){
            LOG.info("inmemory Tree is full , flush inmemory tree to disk, flush count is " + ++flushCount);
            sortedKeysInDisk = flushUtil.flushAvlToDisk(inMemoryTree,sortedKeysInDisk);
        }
        inMemoryTree.insert(key);
    }

    private void forTest(){
        IndexLeafNodeIterator<T> iterator = new IndexLeafNodeIterator<>(sortedKeysInDisk,IndexExtentManager.getInstance());
        while (iterator.hasNext()){
            printRawData(iterator.next());
        }
    }

    abstract protected void printRawData(T t);

    protected abstract void createBPlusTree();

    /**
     * 将跟节点载入内存
     */
    protected abstract void cacheRoot();
}
