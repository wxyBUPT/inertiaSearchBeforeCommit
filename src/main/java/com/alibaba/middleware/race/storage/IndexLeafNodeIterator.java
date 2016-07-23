package com.alibaba.middleware.race.storage;

import java.util.Iterator;
import java.util.LinkedList;

/**
 * Created by xiyuanbupt on 7/21/16.
 * 遍历index 的叶子节点的iterator
 */
public class IndexLeafNodeIterator <T extends Comparable<? super T>> implements Iterator<T>{

    private T next;
    private Iterator<DiskLoc> diskLocIterator;
    private Iterator<T> tIterator;
    private IndexExtentManager indexExtentManager;
    private IndexLeafNode currentIndexLeafNode;

    public IndexLeafNodeIterator(LinkedList<DiskLoc> diskLocs,IndexExtentManager indexExtentManager){
        this.indexExtentManager = indexExtentManager;
        diskLocIterator = diskLocs.iterator();
        if(diskLocIterator.hasNext()){
            currentIndexLeafNode = indexExtentManager.getIndexLeafNodeFromDiskLocForInsert(diskLocIterator.next());
            tIterator = currentIndexLeafNode.iterator();
            if(tIterator.hasNext()){
                next = tIterator.next();
            }else {
                next = null;
            }
        }
        else {
            next = null;
        }
    }

    @Override
    public boolean hasNext() {
        return next != null;
    }

    @Override
    public T next() {
        T res = next;
        if(tIterator.hasNext()){
            next = tIterator.next();
        }
        else{
            next = null;
            while (diskLocIterator.hasNext()){
                DiskLoc diskLoc = diskLocIterator.next();
                currentIndexLeafNode = indexExtentManager.getIndexLeafNodeFromDiskLocForInsert(diskLoc);
                tIterator = currentIndexLeafNode.iterator();
                if(tIterator.hasNext()){
                    next = tIterator.next();
                    break;
                }else {
                    next = null;

                }
            }
        }
        return res;
    }

    @Override
    public void remove() {

    }


}
