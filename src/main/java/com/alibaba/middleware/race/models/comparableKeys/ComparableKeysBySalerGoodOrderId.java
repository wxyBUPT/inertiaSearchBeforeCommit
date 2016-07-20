package com.alibaba.middleware.race.models.comparableKeys;

import com.alibaba.middleware.race.storage.DiskLoc;

import java.io.Serializable;

/**
 * Created by xiyuanbupt on 7/18/16.
 */
public class ComparableKeysBySalerGoodOrderId implements Comparable<ComparableKeysBySalerGoodOrderId>,Serializable{

    private String goodId;
    private Long orderId;
    private DiskLoc diskLoc;

    public ComparableKeysBySalerGoodOrderId(String goodId,Long orderId,DiskLoc diskLoc){
        this.goodId = goodId;
        this.orderId = orderId;
        this.diskLoc = diskLoc;
    }

    @Override
    public int compareTo(ComparableKeysBySalerGoodOrderId o) {
        if(this.goodId==null||this.orderId==null
                ||o.goodId==null||o.orderId==null){
            throw new RuntimeException("Bad keys,there is a bug maybe");
        }
        int ret = this.goodId.compareTo(o.goodId);
        if(ret!=0){
            return ret;
        }
        ret = this.orderId.compareTo(o.orderId);
        return ret;
    }
}
