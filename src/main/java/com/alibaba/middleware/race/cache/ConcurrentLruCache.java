package com.alibaba.middleware.race.cache;


import java.util.Deque;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by xiyuanbupt on 7/24/16.
 */
public abstract class ConcurrentLruCache<KEY, VALUE > implements LRUCache<KEY,VALUE> {

    protected final ReentrantLock lock = new ReentrantLock ();


    protected final Map<KEY, VALUE> map = new ConcurrentHashMap<>();
    protected final Deque<KEY> queue = new LinkedList<>();
    protected final int limit;


    public ConcurrentLruCache(int limit ) {
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
    abstract public VALUE get ( KEY key ) ;


    protected void addKey(KEY key) {
        lock.lock ();
        try {
            queue.addFirst ( key );
        } finally {
            lock.unlock ();
        }


    }

    protected KEY removeLast( ) {
        lock.lock ();
        try {
            final KEY removedKey = queue.removeLast ();
            return removedKey;
        } finally {
            lock.unlock ();
        }
    }

    @Override
    public int getLimit() {
        return limit;
    }

    protected void removeThenAddKey(KEY key) {
        lock.lock ();
        try {
            queue.removeFirstOccurrence ( key );
            queue.addFirst ( key );
        } finally {
            lock.unlock ();
        }

    }

    protected void removeFirstOccurrence(KEY key) {
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

}
