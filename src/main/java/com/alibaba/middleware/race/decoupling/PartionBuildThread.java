package com.alibaba.middleware.race.decoupling;

import com.alibaba.middleware.race.storage.IndexNameSpace;
import com.alibaba.middleware.race.storage.IndexPartition;
import com.alibaba.middleware.race.storage.Indexable;

import java.io.Serializable;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * Created by xiyuanbupt on 7/26/16.
 * 与buildThrea类似,但是为添加了一层hash 之后将数据插入到 indexPartion 中对应的partion
 */
public abstract class PartionBuildThread <T extends Comparable<? super T> & Serializable & Indexable> implements Runnable{

    protected static Logger LOG = Logger.getLogger(PartionBuildThread.class.getName());
    //用来缓存的有界队列
    protected LinkedBlockingDeque<T> keysQueue;
    /**
     * 用来判断原始数据复制线程是否完成数据复制
     */
    protected final AtomicInteger nRemain;

    /**
     * 依旧使用flushUtil 做数据迁移,不过flushUtil 在七月26日之后需要重写
     */
    protected FlushUtil<T> flushUtil;
    protected CountDownLatch sendFinishSingle;

    /**
     * 维持一个indexNameSpace 的引用,用来插入数据
     */
    protected IndexNameSpace indexNameSpace;

    /**
     * 用于记录执行一次flush 之后插入了多少数据
     */

    /**
     * 用于记录总的插入数量
     */
    private Long totalInsertCount;

    /**
     * HashMap 里面的元素是索引中的所有分片
     */
    protected HashMap<Integer,IndexPartition<T>> myPartions;

    /**
     * 构造函数
     * @param nRemain
     * @param sendFinishSingle
     */
    public PartionBuildThread(final AtomicInteger nRemain,CountDownLatch sendFinishSingle){
        totalInsertCount = 0L;
        this.nRemain = nRemain;
        flushUtil = new FlushUtil<>();
        this.sendFinishSingle = sendFinishSingle;
        indexNameSpace = IndexNameSpace.getInstance();
    }

    @Override
    public void run() {
        LOG.info("PartionBuild thread start(May be Order)");
        while(true){
            try{
                T keys = keysQueue.poll(1, TimeUnit.SECONDS);
                if(keys == null){
                    /**
                     * 如果读文件线程全部完成
                     */
                    if(nRemain.get()==0){
                        LOG.info("finish insert all index data of ");
                        break;
                    }
                    /**
                     * 避免空插数据
                     */
                    continue;
                }
                insertKeys(keys);
            }catch (Exception e){
                e.printStackTrace();
                System.exit(-1);
            }
        }
        LOG.info("Create bPlus tree");
        createBPlusTree();
        LOG.info("Finsh create bPlust tree");
        sendFinishSingle.countDown();
        LOG.info("All index data have been insertd,Now enjoy Search !!!");
    }

    private void insertKeys(T key){
        totalInsertCount++;
        putIndexToPartion(key);
    }

    protected abstract void putIndexToPartion(T t);

    /**
     * 在每一个Partion 中创建b+ 树
     */
    protected abstract void createBPlusTree();

    public String getInfo(){
        return "element count insert: " + totalInsertCount;
    }
}
