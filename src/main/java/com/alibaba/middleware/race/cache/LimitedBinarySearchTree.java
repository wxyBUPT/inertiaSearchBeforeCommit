package com.alibaba.middleware.race.cache;

import com.alibaba.middleware.race.models.comparableKeys.ComparableKeysByBuyerCreateTimeOrderId;

/**
 * Created by xiyuanbupt on 7/25/16.
 */
public class LimitedBinarySearchTree<T extends Comparable<? super T>> extends BinarySearchTree<T>{
    private int maxElement;

    public LimitedBinarySearchTree(int maxElement){
        super();
        this.maxElement = maxElement;
    }

    public boolean isFull(){
        return this.maxElement <= this.elementCount;
    }
}
