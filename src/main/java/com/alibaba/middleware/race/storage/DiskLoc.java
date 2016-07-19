package com.alibaba.middleware.race.storage;

import java.io.Serializable;

/**
 * Created by xiyuanbupt on 7/11/16.
 * 记录磁盘位置信息
 * http://docs.ros.org/electric/api/mongodb/html/classmongo_1_1DiskLoc.html#a4e481c6017491a33983cb4b57c1120d8
 */
public class DiskLoc implements Serializable{

    private int _a;//处于哪个文件
    private int ofs;//在文件中的偏移位置
    private StoreType storeType;
    private int size;

    public DiskLoc(int _a,int ofs,StoreType storeType,int size){
        this._a = _a;
        this.ofs = ofs;
        this.storeType = storeType;
        this.size = size;
    }

    public int getOfs(){
        return ofs;
    }

    //通过当前Disk位置获得Extent

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
        sb.append("DiskLoc: fileNum :").append(_a).append(", offset : ").append(ofs).append(",store type: ");
        sb.append(storeType).append(", data size : ").append(size);
        return sb.toString();
    }
}

