package com.alibaba.middleware.race.decoupling;

import com.alibaba.middleware.race.RaceConf;
import com.alibaba.middleware.race.cache.LimitedAvlTree;
import com.alibaba.middleware.race.models.comparableKeys.*;


/**
 * Created by xiyuanbupt on 7/19/16.
 * 内存中的avl 索引
 */
public class InMemoryAvlIndex {
    static int limitedSize = RaceConf.INMEMORYMAXINDEXSIZE;
    public static LimitedAvlTree<ComparableKeysByGoodOrderId> goodOrderIdLimitedAvlTree = new LimitedAvlTree<>(limitedSize);
    public static LimitedAvlTree<ComparableKeysBySalerIdGoodId> salerIdGoodIdLimitedAvlTree = new LimitedAvlTree<>(limitedSize);
    public static LimitedAvlTree<ComparableKeysByBuyerCreateTimeOrderId> buyerCreateTimeOrderIdLimitedAvlTree = new LimitedAvlTree<>(limitedSize);
    public static LimitedAvlTree<ComparableKeysByOrderId> orderIdLimitedAvlTree = new LimitedAvlTree<>(limitedSize);
    public static LimitedAvlTree<ComparableKeysByGoodId> goodIdLimitedAvlTree = new LimitedAvlTree<>(limitedSize);
    public static LimitedAvlTree<ComparableKeysByBuyerId> buyerIdLimitedAvlTree = new LimitedAvlTree<>(limitedSize);
}
