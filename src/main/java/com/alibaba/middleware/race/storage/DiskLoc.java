package com.alibaba.middleware.race.storage;

import java.io.Serializable;

/**
 * Created by xiyuanbupt on 7/11/16.
 * 记录磁盘位置信息
 * http://docs.ros.org/electric/api/mongodb/html/classmongo_1_1DiskLoc.html#a4e481c6017491a33983cb4b57c1120d8
 */
public class DiskLoc implements Serializable{

    /**
     * 属于哪个逻辑Extent
     */
    protected int _a;//处于的逻辑Extent 位置
    /**
     * 在Extent 中的位置
     */
    protected int ofs;//在Extent 中的便宜位置
    /**
     * 存储类型
     */
    protected StoreType storeType;
    /**
     * 数据大小
     */
    protected int size;

    public DiskLoc(int _a,int ofs,StoreType storeType,int size){
        this._a = _a;
        this.ofs = ofs;
        this.storeType = storeType;
        this.size = size;
    }

    public int getOfs(){
        return ofs;
    }

    public int get_a(){
        return _a;
    }

    public StoreType getStoreType(){
        return storeType;
    }

    public int getSize(){
        return this.size;
    }

    public boolean isRowData(){
        return this.storeType.equals(StoreType.ROWDATA);
    }

    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append("DiskLoc: extentNum:").append(_a).append(", offset : ").append(ofs).append(",store type: ");
        sb.append(storeType).append(", data size : ").append(size);
        return sb.toString();
    }

    public void setStoreType(StoreType storeType){
        this.storeType = storeType;
    }
}

