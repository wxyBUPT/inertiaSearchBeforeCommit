package com.alibaba.middleware.race.models;

import com.alibaba.middleware.race.OrderSystem;

import java.io.Serializable;

/**
 * Created by xiyuanbupt on 7/9/16.
 */
public class RowKV implements Comparable<RowKV>,OrderSystem.KeyValue ,Serializable{

    String key;
    String rawValue;
    static private String booleanTrueValue = "true";
    static private String booleanFalseValue = "false";

    boolean isComparableLong = false;
    long longValue;

    public RowKV(String key,String rawValue){
        this.key = key;
        this.rawValue = rawValue;
        if(key.equals("createtime") || key.equals("orderid")){
            isComparableLong = true;
            longValue = Long.parseLong(rawValue);
        }
    }

    public String key(){return key;}

    public String valueAsString(){return rawValue;}

    public long valueAsLong() throws OrderSystem.TypeException {
        try{
            return Long.parseLong(rawValue);
        }catch (Exception e){
            throw new OrderSystem.TypeException();
        }
    }

    @Override
    public double valueAsDouble() throws OrderSystem.TypeException {
        try {
            return Double.parseDouble(rawValue);
        } catch (NumberFormatException e) {
            throw new OrderSystem.TypeException();
        }
    }

    @Override
    public boolean valueAsBoolean() throws OrderSystem.TypeException {
        if (this.rawValue.equals(booleanTrueValue)) {
            return true;
        }
        if (this.rawValue.equals(booleanFalseValue)) {
            return false;
        }
        throw new OrderSystem.TypeException();
    }

    @Override
    public int compareTo(RowKV o) {
        if (!this.key().equals(o.key())) {
            throw new RuntimeException("Cannot compare from different key");
        }
        if (isComparableLong) {
            return Long.compare(this.longValue, o.longValue);
        }
        return this.rawValue.compareTo(o.rawValue);
    }

    @Override
    public String toString(){
        return "[" + this.key + "]:" + this.rawValue;
    }
}
