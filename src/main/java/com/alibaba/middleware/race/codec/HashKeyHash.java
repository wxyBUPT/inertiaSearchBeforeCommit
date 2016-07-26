package com.alibaba.middleware.race.codec;

import com.alibaba.middleware.race.RaceConf;

/**
 * Created by xiyuanbupt on 7/26/16.
 * 只有一个函数,将不同的key 值hash 到不同的分片
 */
public class HashKeyHash {

    public static int hashKeyHash(int hashCode){
        return hashCode% RaceConf.N_PARTITION;
    }

    public static void main(String[] args){
        for(int i = 126;i<234;i++){
            System.out.println(HashKeyHash.hashKeyHash(i));
        }
    }
}
