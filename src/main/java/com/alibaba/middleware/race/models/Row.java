package com.alibaba.middleware.race.models;

import java.io.Serializable;
import java.util.HashMap;

/**
 * Created by xiyuanbupt on 7/9/16.
 */
public class Row extends HashMap<String,RowKV> implements Serializable{

    public Row(){
        super();
    }

    public Row(RowKV kv){
        super();
        this.put(kv.key(),kv);
    }

    RowKV getKV(String key){
        RowKV kv = this.get(key);
        if(kv==null){
            throw new RuntimeException(key+" is not exist");
        }
        return kv;
    }

    Row putKV(String key,String value){
        RowKV rowKV = new RowKV(key,value);
        this.put(rowKV.key(),rowKV);
        return this;
    }

    Row putKV(String key,long value){
        RowKV rowKV = new RowKV(key,Long.toString(value));
        this.put(rowKV.key(),rowKV);
        return this;
    }
}
