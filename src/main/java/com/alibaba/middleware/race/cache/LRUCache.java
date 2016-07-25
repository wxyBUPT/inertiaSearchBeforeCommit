package com.alibaba.middleware.race.cache;


/**
 * Created by xiyuanbupt on 7/24/16.
 */
public interface LRUCache<KEY,VALUE> {
    void put ( KEY key, VALUE value );

    VALUE get ( KEY key );

    void remove ( KEY key );

    int size ();

    int getLimit();
}
