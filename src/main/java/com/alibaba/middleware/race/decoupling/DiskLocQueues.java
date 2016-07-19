package com.alibaba.middleware.race.decoupling;

import com.alibaba.middleware.race.models.comparableKeys.*;

import java.util.concurrent.LinkedBlockingDeque;

/**
 * Created by xiyuanbupt on 7/18/16.
 * 当待排序的行插入到文件之后,会在DiskLocQueues 中缓存key 值,以及对应的DiskLoc
 */
public class DiskLocQueues {

    public static final LinkedBlockingDeque<ComparableKeysByBuyerId> comparableKeysByBuyerIdQueue = new LinkedBlockingDeque<>(100000);
    public static final LinkedBlockingDeque<ComparableKeysByGoodId> comparableKeysByGoodIdQueue = new LinkedBlockingDeque<>(100000);
    public static final LinkedBlockingDeque<ComparableKeysByOrderId> comparableKeysByOrderId = new LinkedBlockingDeque<>(100000);

    public static final LinkedBlockingDeque<ComparableKeysBySalerIdGoodId> comparableKeysBySalerIdGoodId = new LinkedBlockingDeque<>(100000);

    public static final LinkedBlockingDeque<ComparableKeysByBuyerCreateTimeOrderId> comparableKeysByBuyerCreateTimeOrderId = new LinkedBlockingDeque<>(100000);
    public static final LinkedBlockingDeque<ComparableKeysBySalerGoodOrderId> comparableKeysBySalerGoodOrderId = new LinkedBlockingDeque<>(100000);
    public static final LinkedBlockingDeque<ComparableKeysByGoodOrderId> comparableKeysByGoodOrderId = new LinkedBlockingDeque<>(100000);
}
