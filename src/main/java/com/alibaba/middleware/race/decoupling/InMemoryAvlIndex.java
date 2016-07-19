package com.alibaba.middleware.race.decoupling;

import com.alibaba.middleware.race.cache.LimitedAvlTree;
import com.alibaba.middleware.race.models.comparableKeys.*;


/**
 * Created by xiyuanbupt on 7/19/16.
 * 内存中的avl 索引
 */
public class InMemoryAvlIndex {
    public static LimitedAvlTree<ComparableKeysByGoodOrderId> goodOrderIdLimitedAvlTree;
    public static LimitedAvlTree<ComparableKeysBySalerIdGoodId> salerIdGoodIdLimitedAvlTree;
    public static LimitedAvlTree<ComparableKeysByBuyerCreateTimeOrderId> buyerCreateTimeOrderIdLimitedAvlTree;
    public static LimitedAvlTree<ComparableKeysBySalerGoodOrderId> salerGoodOrderIdLimitedAvlTree;
    public static LimitedAvlTree<ComparableKeysByOrderId> orderIdLimitedAvlTree;
    public static LimitedAvlTree<ComparableKeysByGoodId> goodIdLimitedAvlTree;
    public static LimitedAvlTree<ComparableKeysByBuyerId> buyerIdLimitedAvlTree;
}
