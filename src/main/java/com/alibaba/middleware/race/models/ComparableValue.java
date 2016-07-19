package com.alibaba.middleware.race.models;

import java.io.Serializable;
import java.util.HashMap;

/**
 * Created by xiyuanbupt on 7/13/16.
 */
public class ComparableValue extends HashMap<String,RowKV> implements Serializable{

    ComparableValue(RowKV rowKV){
        super();
        this.put(rowKV.key(),rowKV);
    }


}
