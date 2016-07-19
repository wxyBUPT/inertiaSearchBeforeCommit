package com.alibaba.middleware.race.index;

import com.alibaba.middleware.race.cache.AvlTree;

import java.util.concurrent.LinkedBlockingDeque;

/**
 * Created by xiyuanbupt on 7/19/16.
 */
public class InmemoryIndex<T extends Comparable<? super T>> implements Runnable{

    private LinkedBlockingDeque<T> keysQueue;
    private AvlTree<T> memoryIndex;

    @Override
    public void run() {
        while(true){

        }
    }
}
