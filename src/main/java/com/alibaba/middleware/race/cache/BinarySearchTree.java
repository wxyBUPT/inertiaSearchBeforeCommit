package com.alibaba.middleware.race.cache;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Created by xiyuanbupt on 7/25/16.
 */
public class BinarySearchTree<T extends Comparable<? super T>> implements Iterable<T>{

    protected BinarySearchNode<T> root;

    //插入数量与当前节点元素数量
    protected long countInsertions;
    protected int elementCount;

    public String getInfo(){
        StringBuilder sb = new StringBuilder();
        sb.append("Memory BinarySearch Tree#### countInsertions: ").append(countInsertions);
        sb.append(", element: ").append(elementCount);
        return sb.toString();
    }

    public BinarySearchTree(){
        root = null;
        countInsertions = 0L;
        elementCount = 0;
    }

    /**
     * Insert an element into the tree
     * @param x
     * @return
     */
    public boolean insert(T x){
        try {
            root = insert(x, root, null);
            elementCount++;
            countInsertions++;
            return true;
        }catch (Exception e){
            e.printStackTrace();
            return false;
        }
    }

    public void makeEmpty(){
        elementCount = 0;
        root = null;
    }

    /**
     * Internal method to perform an actual insertion.
     * @param x
     * @return
     */
    public BinarySearchNode<T> insert(T x, BinarySearchNode<T> t,BinarySearchNode<T> parent) throws Exception{
        if(t == null){
            t = new BinarySearchNode<>(x);
            t.parent = parent;
        }
        else if(x.compareTo(t.element)<0){
            t.left = insert(x,t.left,t);
        }else if(x.compareTo(t.element)>0){
            t.right = insert(x,t.right,t);
        }else {
            System.out.println("Some error Happen");
            System.out.println(t.element);
            throw new Exception("Attempting to insert duplicate value ");
        }
        return t;
    }

    @Override
    public Iterator<T> iterator() {
        return new TreeIterator(root);
    }
    class TreeIterator implements Iterator<T>{
        private BinarySearchNode<T> next;

        public TreeIterator(BinarySearchNode<T> root){
            next = root;
            if(next == null)
                return;
            while(next.left!=null){
                next = next.left;
            }
        }

        public boolean hasNext(){
            return next != null;
        }

        public T next(){
            if(!hasNext())throw new NoSuchElementException();
            BinarySearchNode<T> r = next;
            if(next.right != null){
                next = next.right;
                while (next.left != null)
                    next = next.left;
                return r.getElement();
            }else while(true){
                if(next.parent == null){
                    next = null;
                    return r.getElement();
                }
                if(next.parent.left == next){
                    next = next.parent;
                    return r.getElement();
                }
                next = next.parent;
            }
        }

        @Override
        public void remove(){

        }
    }
    public static void main (String[] args) { //String []args){
        BinarySearchTree<Integer> t = new BinarySearchTree<>();

        t.insert(new Integer(2));
        t.insert(new Integer(1));
        t.insert(new Integer(4));
        t.insert(new Integer(5));
        t.insert(new Integer(9));
        t.insert(new Integer(3));
        t.insert(new Integer(6));
        t.insert(new Integer(7));
        t.insert(new Integer(8));
        for(Integer i:t){
            System.out.println(i);
        }
    }
}

class BinarySearchNode<T>{
    protected T element;
    protected BinarySearchNode<T> left;
    protected BinarySearchNode<T> right;

    /**
     * 父节点
     */
    protected BinarySearchNode<T> parent;

    public BinarySearchNode(T element){
        this(element,null,null);
    }

    public BinarySearchNode(T element,BinarySearchNode<T> lt,BinarySearchNode<T> rt){
        this.element = element;
        this.left = lt;
        this.right = rt;
    }

    public T getElement(){
        return element;
    }

    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append("element: ").append(element);
        sb.append(", left: ").append(left);
        sb.append(", right: ").append(right);
        sb.append(", parent: ").append(parent);
        return sb.toString();
    }
}