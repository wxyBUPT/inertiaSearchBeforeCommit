package com.alibaba.middleware.race.storage;

import com.alibaba.middleware.race.RaceConf;
import com.alibaba.middleware.race.cache.ConcurrentLruCacheForBigData;
import com.alibaba.middleware.race.cache.LRUCache;
import com.alibaba.middleware.race.decoupling.FlushUtil;
import com.alibaba.middleware.race.models.Row;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.CountDownLatch;

/**
 * Created by xiyuanbupt on 7/26/16.
 * 一类 index 的一个分片,负责管理hash() 值相同的
 * 重构之后, 很多indexNameSpace 的工作交给了partion 处理,indexNameSpace 只负责路由的功能
 */
public class IndexPartition<T extends Comparable<? super T> & Serializable & Indexable> {

    /**
     * 一类Key 值被分为64部分
     */
    private final int myHashCode;

    /**
     * Vector 中每一个元素都是在磁盘中排好序的key 值,此部分
     */
    private Queue<LinkedList<DiskLoc>> sortedKeysInDisk;

    /**
     * 用来缓存b+ 树的跟节点
     */
    public IndexNode<T> rootIndex;

    /**
     * 查询阶段的缓存放在partition 中
     */
    private final LRUCache<DiskLoc,IndexNode> myLRU;

    /**
     * 两个用于和底层db 交互的代理
     */
    private IndexExtentManager indexExtentManager;
    private StoreExtentManager storeExtentManager;

    private FlushUtil<T> flushUtil;

    /**
     * 构造函数
     * @param myHashCode
     */
    public IndexPartition(int myHashCode){
        indexExtentManager = IndexExtentManager.getInstance();
        storeExtentManager = StoreExtentManager.getInstance();
        this.myHashCode = myHashCode;
        this.sortedKeysInDisk = new LinkedList<>();
        /**
         * 假定当前只对 order key 的值分片
         */
        myLRU = new ConcurrentLruCacheForBigData<>(RaceConf.N_ORDER_INDEX_CACHE_COUNT);
        flushUtil = new FlushUtil<>();
    }

    /**
     * 磁盘中插入key 数据由外层负责,partion 只负责缓存key 数据在磁盘中插入的有序位置,并提供保存接口
     * @param sortedKeysInDisk
     */
    public void addSortedKeys(LinkedList<DiskLoc> sortedKeysInDisk){
        this.sortedKeysInDisk.add(sortedKeysInDisk);
    }

    /**
     * 归并合并 sortedKeysInDisk,并创建b树
     * 由于partion 之间是没有共享数据的,所以以线程为单位执行
     * 当前是两路归并排序,
     * @param countDownLatch
     */
    public void merageAndBuildMe(final CountDownLatch countDownLatch){
        new Thread(new Runnable() {
            @Override
            public void run() {
                /**
                 * sortedKeysInDisk 中所有的值进行归并排序
                 */
                while(sortedKeysInDisk.size()>1){
                    LinkedList<DiskLoc> diskLocs = sortedKeysInDisk.poll();
                    LinkedList<DiskLoc> diskLocs1 = sortedKeysInDisk.poll();
                    IndexLeafNodeIterator<T> iterator = new IndexLeafNodeIterator<>(diskLocs,indexExtentManager);
                    IndexLeafNodeIterator<T> iterator1 = new IndexLeafNodeIterator<>(diskLocs1,indexExtentManager);
                    sortedKeysInDisk.add(flushUtil.mergeIterator(iterator,iterator1));

                }
                System.out.println("myHashCode is : " + myHashCode + "sortedKeysInDis size is " + sortedKeysInDisk.size());
                System.out.println("我将是整个工程最消耗时间,最消耗空间,最消耗磁盘io 的一部分,请优化我!!!!");
                System.out.println("我还没有实现,但是我可以用于测试!!!!!!!!!");
                countDownLatch.countDown();
            }
        }).start();
    }



    /**
     * 查询一个元素
     * @param t
     * @return
     */
    public Row queryByKey(T t){
        IndexNode<T> indexNode = rootIndex;
        while(!indexNode.isLeafNode()){
            DiskLoc diskLoc = indexNode.search(t);
            if(diskLoc == null )return null;

            IndexNode cacheNode = myLRU.get(diskLoc);
            indexNode = cacheNode==null?indexExtentManager.getIndexNodeFromDiskLoc(diskLoc):cacheNode;
            /**
             * 如果节点是非叶子节点,并且缓存中没有数据,则缓存
             */
        if(cacheNode==null&&!indexNode.isLeafNode())myLRU.put(diskLoc,indexNode);
        }
        DiskLoc diskLoc = indexNode.search(t);
        if(diskLoc==null)return null;
        return storeExtentManager.getRowFromDiskLoc(diskLoc);
    }

    public Deque<Row> rangeQuery(T startKey,T endKey) {
        /**
         * 对磁盘中进行层序遍历
         */
        return levelTraversal(rootIndex,startKey,endKey);
    }

    /**
     * 范围寻找
     * @param root
     * @param minKey
     * @param maxKey
     * @param <V>
     * @return
     */
    private <V extends Comparable&Serializable&Indexable> LinkedList<Row> levelTraversal(IndexNode root, V minKey, V maxKey){
        LinkedList<Row> result = new LinkedList<>();

        Queue<IndexNode> nodes = new LinkedList<>();
        nodes.add(root);
        while(!nodes.isEmpty()){
            IndexNode node = nodes.remove();
            if(node.isLeafNode()){
                Queue<DiskLoc> diskLocs = node.searchBetween(minKey,maxKey);
                DiskLoc diskLoc = diskLocs.poll();
                while(diskLoc!=null){
                    Row row = storeExtentManager.getRowFromDiskLoc(diskLoc);
                    result.add(row);
                    diskLoc = diskLocs.poll();
                }
            }else {
                Queue<DiskLoc> diskLocs = node.searchBetween(minKey,maxKey);
                if(diskLocs!=null) {
                    while (!diskLocs.isEmpty()) {
                        DiskLoc diskLoc = diskLocs.remove();
                        IndexNode cacheNode = myLRU.get(diskLoc);
                        IndexNode indexNode = cacheNode==null?indexExtentManager.getIndexNodeFromDiskLoc(diskLoc):cacheNode;
                        if(cacheNode==null&& !indexNode.isLeafNode()){
                            myLRU.put(diskLoc,indexNode);
                        }
                        nodes.add(indexNode);
                    }
                }
            }
        }
        return result;
    }

}
