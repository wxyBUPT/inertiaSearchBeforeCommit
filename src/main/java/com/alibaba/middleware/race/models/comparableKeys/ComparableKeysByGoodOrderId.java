package com.alibaba.middleware.race.models.comparableKeys;

import com.alibaba.middleware.race.storage.DiskLoc;

import java.io.Serializable;

/**
 * Created by xiyuanbupt on 7/18/16.
 */
public class ComparableKeysByGoodOrderId implements Comparable<ComparableKeysByGoodOrderId>,Serializable{

    private String goodId;
    private Long orderId;
    private DiskLoc diskLoc;

    public ComparableKeysByGoodOrderId(String goodId,Long orderId,DiskLoc diskLoc){
        this.goodId = goodId;
        this.orderId = orderId;
        this.diskLoc = diskLoc;
    }

    public ComparableKeysByGoodOrderId(String goodId,Long orderId){
        this.goodId = goodId;
        this.orderId = orderId;
    }

    @Override
    public int compareTo(ComparableKeysByGoodOrderId o) {
        if(this.goodId==null||this.orderId==null||o.goodId==null||o.orderId==null){
            throw new RuntimeException("Bad keys, there is a bug maybe");
        }

        int ret = this.goodId.compareTo(o.goodId);
        if(ret != 0){
            return ret;
        }
        ret = this.orderId.compareTo(o.orderId);
        return ret;
    }

    public DiskLoc getDiskLoc(){
        return diskLoc;
    }
}
