package com.alibaba.middleware.race.cache;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Created by xiyuanbupt on 7/14/16.
 *
 */
public class AvlTree<T extends Comparable<? super T>> implements Iterable<T>{

    protected AvlNode<T> root;

    // TODO: make these optional based on some sort of 'debug' flag?
    // at the very least, make them read-only properties
    protected long countInsertions;
    protected int elementCount ;

    /**
     * Avl Tree Constructor.
     *
     * Creates an empty tree
     */
    public AvlTree (){
        root = null;
        countInsertions = 0;
    }
    /**
     * Find the maximum value among the given numbers.
     *
     * @param a First number
     * @param b Second number
     * @return Maximum value
     */
    public int max (int a, int b){
        if (a > b)
            return a;
        return b;
    }

    public int getElementCount(){
        return elementCount;
    }

    /**
     * Deletes all nodes from the tree
     */
    public void makeEmpty(){
        elementCount = 0;
        root = null;
    }

    /**
     * Insert an element into the tree.
     *
     * @param x Element to insert into the tree
     * @return True - Success, the Element was added.
     *         False - Error, the element was a duplicate.
     */
    public boolean insert (T x){
        try {
            root = insert (x, root,null);
            elementCount++;
            countInsertions++;
            return true;
        } catch(Exception e){ // TODO: catch a DuplicateValueException instead!
            return false;
        }
    }
    /**
     * Internal method to perform an actual insertion.
     *
     * @param x Element to add
     * @param t Root of the tree
     * @return New root of the tree
     * @throws Exception
     */
    protected AvlNode<T> insert (T x, AvlNode<T> t,AvlNode<T> parent) throws Exception{
        if (t == null) {
            t = new AvlNode<T>(x);
            t.parent = parent;
        }
        else if (x.compareTo (t.element) < 0){
            t.left = insert (x, t.left,t);

            if (height (t.left) - height (t.right) == 2){
                if (x.compareTo (t.left.element) < 0){
                    t = rotateWithLeftChild (t);
                }
                else {
                    t = doubleWithLeftChild (t);
                }
            }
        }
        else if (x.compareTo (t.element) > 0){
            t.right = insert (x, t.right,t);

            if ( height (t.right) - height (t.left) == 2)
                if (x.compareTo (t.right.element) > 0){
                    t = rotateWithRightChild (t);
                }
                else{
                    t = doubleWithRightChild (t);
                }
        }
        else {
            System.out.println("Some error Happen");
            System.out.println(x);
            System.out.println(t.element);
            throw new Exception("Attempting to insert duplicate value");
        }

        t.height = max (height (t.left), height (t.right)) + 1;
        return t;
    }
    /**
     * Rotate binary tree node with left child.
     * For AVL trees, this is a single rotation for case 1.
     * Update heights, then return new root.
     *
     * @param k2 Root of tree we are rotating
     * @return New root
     */
    protected AvlNode<T> rotateWithLeftChild (AvlNode<T> k2){
        AvlNode<T> k1 = k2.left;
        k1.parent = k2.parent;
        k2.parent = k1;
        if(k1.right != null) {
            k1.right.parent = k2;
        }

        k2.left = k1.right;
        k1.right = k2;

        k2.height = max (height (k2.left), height (k2.right)) + 1;
        k1.height = max (height (k1.left), k2.height) + 1;

        return (k1);
    }

    /**
     * Double rotate binary tree node: first left child
     * with its right child; then node k3 with new left child.
     * For AVL trees, this is a double rotation for case 2.
     * Update heights, then return new root.
     *
     * @param k3 Root of tree we are rotating
     * @return New root
     */
    protected AvlNode<T> doubleWithLeftChild (AvlNode<T> k3){
        k3.left = rotateWithRightChild(k3.left);
        return rotateWithLeftChild (k3);
    }

    /**
     * Rotate binary tree node with right child.
     * For AVL trees, this is a single rotation for case 4.
     * Update heights, then return new root.
     *
     * @param k1 Root of tree we are rotating.
     * @return New root
     */
    protected AvlNode<T> rotateWithRightChild (AvlNode<T> k1){
        AvlNode<T> k2 = k1.right;

        k2.parent = k1.parent;
        k1.parent = k2;
        if(k2.left!=null) {
            k2.left.parent = k1;
        }
        k1.right = k2.left;
        k2.left = k1;

        k1.height = max (height (k1.left), height (k1.right)) + 1;
        k2.height = max (height (k2.right), k1.height) + 1;

        return (k2);
    }

    /**
     * Double rotate binary tree node: first right child
     * with its left child; then node k1 with new right child.
     * For AVL trees, this is a double rotation for case 3.
     * Update heights, then return new root.
     *
     * @param k1 Root of tree we are rotating
     * @return New root
     */
    protected AvlNode<T> doubleWithRightChild (AvlNode<T> k1){
        k1.right = rotateWithLeftChild (k1.right);
        return rotateWithRightChild (k1);
    }

    /**
     * Determine the height of the given node.
     *
     * @param t Node
     * @return Height of the given node.
     */
    public int height (AvlNode<T> t){
        return t == null ? -1 : t.height;
    }

    public void inOrder(){
        inOrder(root);
    }

    protected void inOrder(AvlNode<T> node){
        if(node!=null){
            inOrder(node.left);
            System.out.println(node);
            inOrder(node.right);
        }
    }

    public T find(T data){
        return find(data,root);
    }

    protected T find(T data,AvlNode<T> root){
        if(root == null){
            return null;
        }
        if(root.getElement().equals(data)){
            return root.getElement();
        }else if(root.getElement().compareTo(data)<0){
            return find(data,root.right);
        }else {
            return find(data,root.left);
        }
    }

    @Override
    public Iterator<T> iterator(){
        return new TreeIterator(root);
    }

    class TreeIterator implements Iterator<T>{
        private AvlNode<T> next;

        public TreeIterator(AvlNode<T> root){
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
            AvlNode<T> r = next;
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
        AvlTree<Integer> t = new AvlTree<>();

        t.insert(new Integer(2));
        t.insert(new Integer(1));
        t.insert(new Integer(4));
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

class AvlNode<T>{
    protected T element;
    protected AvlNode<T> left;
    protected AvlNode<T> right;
    /**
     *为AvlNode 添加父节点
     */
    protected AvlNode<T> parent;
    //Height of node
    protected int height;
    public AvlNode(T theElement){
        this(theElement,null,null);
    }
    public T getElement(){
        return element;
    }

    public AvlNode(T theElement,AvlNode<T> lt,AvlNode<T> rt){
        element = theElement;
        left = lt;
        right = rt;
    }

    public int getHeight(){
        return height;
    }
    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append("element:").append(element);
        sb.append(",height is ").append(height);
        if(parent!=null){
            sb.append("parent is:").append(parent.getElement());
        }else {
            sb.append("parent is Null");
        }
        //sb.append(", height is: ").append(height).append(", parent is :").append(parent);
        return sb.toString();
    }
}
