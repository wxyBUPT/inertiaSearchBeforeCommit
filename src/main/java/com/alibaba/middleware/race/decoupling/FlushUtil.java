package com.alibaba.middleware.race.decoupling;

import com.alibaba.middleware.race.cache.AvlTree;
import com.alibaba.middleware.race.storage.*;

import java.io.Serializable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * Created by xiyuanbupt on 7/19/16.
 * 提供工具类,将本地缓存的索引同步到磁盘
 */
public class FlushUtil<T extends Comparable<? super T> & Serializable> {

    private static Logger LOG = Logger.getLogger(FlushUtil.class.getName());
    private AtomicInteger flushCount ;

    private IndexExtentManager indexExtentManager;
    public LinkedList<DiskLoc> flushAvlToDisk(AvlTree<T> from , LinkedList<DiskLoc> diskLocs){
        LOG.info("Strat flush, notice if this type of log is too much ,You must can't pass, current flush Count is " +
        flushCount.incrementAndGet());
        LinkedList<DiskLoc> newDiskLocs = new LinkedList<>();
        Iterator<T> fromIterator = from.iterator();
        Iterator<T> leafNodeTIterator = new IndexLeafNodeIterator<>(diskLocs,indexExtentManager);

        IndexLeafNode<T> currentIndexLeaf = new IndexLeafNode<>();

        T fromNext;
        if(fromIterator.hasNext()){
            fromNext = fromIterator.next();
        }else {
            fromNext = null;
        }

        T toNext;
        if(leafNodeTIterator.hasNext()){
            LOG.info("Disk have data, this is not first flush");
            toNext = leafNodeTIterator.next();
        }else {
            LOG.info("Disk don't have data , this is first flush");
            toNext = null;
        }

        while(fromNext!=null&&toNext!=null){
            if(currentIndexLeaf.isFull()){
                DiskLoc diskLoc = indexExtentManager.putIndexLeafNode(currentIndexLeaf);
                newDiskLocs.add(diskLoc);
                currentIndexLeaf = new IndexLeafNode<>();
            }
            int ret = fromNext.compareTo(toNext);
            if(ret<0){
                currentIndexLeaf.appendData(fromNext);
                if(fromIterator.hasNext()){
                    fromNext = fromIterator.next();
                }else {
                    fromNext = null;
                }
            }else if(ret>0){
                currentIndexLeaf.appendData(toNext);
                if(leafNodeTIterator.hasNext()){
                    toNext = leafNodeTIterator.next();
                }else {
                    toNext = null;
                }
            }else {
                 //key值相同,一定是出现了bug
                LOG.info("Some bug happen , key is same");
                System.exit(-1);
            }
        }

        while(fromNext!=null){
            if(currentIndexLeaf.isFull()){
                DiskLoc diskLoc = indexExtentManager.putIndexLeafNode(currentIndexLeaf);
                newDiskLocs.add(diskLoc);
                currentIndexLeaf = new IndexLeafNode<>();
            }
            currentIndexLeaf.appendData(fromNext);
            if(fromIterator.hasNext()){
                fromNext = fromIterator.next();
            }else {
                fromNext = null;
            }
        }

        while (toNext!=null){
            if(currentIndexLeaf.isFull()){
                DiskLoc diskLoc = indexExtentManager.putIndexLeafNode(currentIndexLeaf);
                newDiskLocs.add(diskLoc);
                currentIndexLeaf = new IndexLeafNode<>();
            }
            currentIndexLeaf.appendData(toNext);
            if(leafNodeTIterator.hasNext()){
                toNext = leafNodeTIterator.next();
            }else {
                toNext = null;
            }
        }

        DiskLoc diskLoc = indexExtentManager.putIndexLeafNode(currentIndexLeaf);
        newDiskLocs.add(diskLoc);

        from.makeEmpty();
        return newDiskLocs;
    }


    /**
     * 排序完成之后统一创建b+,比较耗时
     * @param diskLocs
     * @return
     */
    public DiskLoc buildBPlusTree(LinkedList<DiskLoc> diskLocs){
        LOG.info("Start build bPlusTree");
        LinkedList<DiskLoc> thisLevelNodePostion = diskLocs;
        LinkedList<DiskLoc> highLevelNodePosition;
        IndexTreeNode<T> currentParent = new IndexTreeNode<>();
        while(thisLevelNodePostion.size()!=1){
            System.out.println(thisLevelNodePostion.size());
            highLevelNodePosition = new LinkedList<>();
            for(DiskLoc diskLoc:thisLevelNodePostion){
                /**
                 * If currentParent is full , save it to disk, insert position to highLevelNodePosition
                 */
                if(currentParent.isFull()){
                    DiskLoc insertPosition = indexExtentManager.putIndexNode(currentParent);
                    highLevelNodePosition.add(insertPosition);
                    currentParent = new IndexTreeNode<>();
                }
                IndexNode<T> indexNode = indexExtentManager.getIndexNodeFromDiskLoc(diskLoc);
                T minKey = indexNode.getMinKey();
                System.out.println(minKey);
                currentParent.appendData(minKey);
                currentParent.addPointer(diskLoc);
            }
            /**
             * Insert the latest indexNode
             */
            DiskLoc insertPosition = indexExtentManager.putIndexNode(currentParent);
            highLevelNodePosition.add(insertPosition);
            currentParent = new IndexTreeNode<>();
            thisLevelNodePostion = highLevelNodePosition;
        }
        return thisLevelNodePostion.getFirst();
    }

    public FlushUtil(){
        this.indexExtentManager = IndexExtentManager.getInstance();
        flushCount = new AtomicInteger(0);
    }
}
