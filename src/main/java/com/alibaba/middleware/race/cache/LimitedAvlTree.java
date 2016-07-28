package com.alibaba.middleware.race.cache;

/**
 * Created by xiyuanbupt on 7/19/16.
 */
public class LimitedAvlTree<T extends Comparable<? super T>> extends AvlTree<T > {

    private int maxElement;

    public LimitedAvlTree(int maxElement){
        super();
        this.maxElement = maxElement;
    }

    public synchronized boolean isFull(){
        return this.maxElement<= this.elementCount;
    }

    public static void main (String[] args) { //String []args){
        LimitedAvlTree<Integer> t = new LimitedAvlTree<>(3);

        t.insert(new Integer(2));
        t.insert(new Integer(1));
        System.out.println(t.isFull());
        t.insert(new Integer(4));
        System.out.println(t.isFull());
        t.insert(new Integer(5));
        t.insert(new Integer(9));
        t.insert(new Integer(3));
        t.insert(new Integer(6));
        t.insert(new Integer(7));
        t.insert(new Integer(8));
        t.inOrder();
        for(Integer i:t){
            System.out.println(i);
        }
        System.out.println(t.find(4));
        System.out.println(t.find(8));
        System.out.println(t.find(10));
    }
}
