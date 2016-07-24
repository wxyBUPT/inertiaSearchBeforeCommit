package com.alibaba.middleware.race.cache;

import com.alibaba.middleware.race.storage.IndexNode;

import java.util.Deque;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by xiyuanbupt on 7/24/16.
 */
public class ConcurrentLruCacheForMinData<KEY, VALUE extends IndexNode> implements LRUCache<KEY,VALUE> {

    private final ReentrantLock lock = new ReentrantLock ();


    private final Map<KEY, VALUE> map = new ConcurrentHashMap<>();
    private final Deque<KEY> queue = new LinkedList<>();
    private final int limit;


    public ConcurrentLruCacheForMinData(int limit ) {
        this.limit = limit;
    }

    @Override
    public void put ( KEY key, VALUE value ) {
        VALUE oldValue = map.put ( key, value );
        if ( oldValue != null ) {
            removeThenAddKey ( key );
        } else {
            addKey ( key );
        }
        if (map.size () > limit) {
            map.remove ( removeLast() );
        }
    }


    @Override
    public VALUE get ( KEY key ) {
        VALUE value = map.get(key);
        /**
         * 非叶子节点优先级更高
         */
        if(value!=null && !value.isLeafNode()) {
            removeThenAddKey(key);
        }
        return value;
    }


    private void addKey(KEY key) {
        lock.lock ();
        try {
            queue.addFirst ( key );
        } finally {
            lock.unlock ();
        }


    }

    private KEY removeLast( ) {
        lock.lock ();
        try {
            final KEY removedKey = queue.removeLast ();
            return removedKey;
        } finally {
            lock.unlock ();
        }
    }

    private void removeThenAddKey(KEY key) {
        lock.lock ();
        try {
            queue.removeFirstOccurrence ( key );
            queue.addFirst ( key );
        } finally {
            lock.unlock ();
        }

    }

    private void removeFirstOccurrence(KEY key) {
        lock.lock ();
        try {
            queue.removeFirstOccurrence ( key );
        } finally {
            lock.unlock ();
        }

    }

    @Override
    public void remove ( KEY key ) {
        removeFirstOccurrence ( key );
        map.remove ( key );
    }

    @Override
    public int size () {
        return map.size ();
    }

    public String toString () {
        return map.toString ();
    }

    public static void main(String[] args){
        LRUCache<Integer, IndexNode> cache = new ConcurrentLruCacheForMinData<>( 4 );
        System.out.println(cache.get(1));
    }
}
