package com.alibaba.middleware.race.cache;

/**
 * Created by xiyuanbupt on 7/14/16.
 */
public class AvlTree<T extends Comparable<? super T>> {
    public AvlNode<T> root;

    // TODO: make these optional based on some sort of 'debug' flag?
    // at the very least, make them read-only properties
    public int countInsertions;
    public int countSingleRotations;
    public int countDoubleRotations;
    public AvlTree(){
        root = null;
        countInsertions = 0;
        countSingleRotations = 0;
        countDoubleRotations = 0;
    }
    /**
     * Determine the height of the given node.
     */
    public int height(AvlNode<T> t){
        return t == null ? -1:t.height;
    }
    /**
     * Find the maximum value among the given numbers.
     */
    public int max(int a, int b){
        if(a>b)return a;
        return b;
    }

    /**
     * Insert an element into the tree.
     */
    public boolean insert(T x){
        try{
            root = insert(x,root);

            countInsertions ++;
            return true;
        }catch (Exception e){
            return false;
        }
    }
    /**
     * Internal method to perform an actual insertion.
     */
    protected AvlNode<T> insert(T x,AvlNode<T> t)throws Exception{
        if(t==null)
            t = new AvlNode<T>(x);
        else if(x.compareTo(t.element)<0){
            t.left = insert(x,t.left);

            if(height(t.left) - height(t.right) == 2){
                if(x.compareTo(t.left.element)<0){
                    t = rotateWithLeftChild(t);
                    countSingleRotations ++;
                }
                else {
                    t = doubleWithLeftChild(t);
                    countDoubleRotations ++;
                }
            }
        }
        else if(x.compareTo(t.element)>0){
            t.right = insert(x,t.right);
            if ( height (t.right) - height (t.left) == 2)
                if (x.compareTo (t.right.element) > 0){
                    t = rotateWithRightChild (t);
                    countSingleRotations++;
                }
                else{
                    t = doubleWithRightChild (t);
                    countDoubleRotations++;
                }
        }
        else {
            throw new Exception("Attempting to insert duplicate value");
        }
        t.height = max(height(t.left),height(t.right)) +1;
        return t;
    }

    /**
     * Rotate binary tree node with left child.
     * Update heights, then return new root
     */
    protected AvlNode<T> rotateWithLeftChild(AvlNode<T> k2){
        AvlNode<T> k1 = k2.left;

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
        k3.left = rotateWithRightChild (k3.left);
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

    public void makeEmpty(){
        root = null;
    }

    public boolean isEmpty(){
        return (root == null);
    }
    /**
     * Find the smallest item in the tree.
     * @return smallest item or null if empty.
     */
    public T findMin( )
    {
        if( isEmpty( ) ) return null;

        return findMin( root ).element;
    }

    /**
     * Find the largest item in the tree.
     * @return the largest item of null if empty.
     */
    public T findMax( )
    {
        if( isEmpty( ) ) return null;
        return findMax( root ).element;
    }

    /**
     * Internal method to find the smallest item in a subtree.
     * @param t the node that roots the tree.
     * @return node containing the smallest item.
     */
    private AvlNode<T> findMin(AvlNode<T> t)
    {
        if( t == null )
            return t;

        while( t.left != null )
            t = t.left;
        return t;
    }

    /**
     * Internal method to find the largest item in a subtree.
     * @param t the node that roots the tree.
     * @return node containing the largest item.
     */
    private AvlNode<T> findMax( AvlNode<T> t )
    {
        if( t == null )
            return t;

        while( t.right != null )
            t = t.right;
        return t;
    }

    public void inOrder(){
        inOrder(root);
    }

    protected void inOrder(AvlNode T){
        if(T!= null){
            inOrder(T.left);
            System.out.println(T.getElement());
            inOrder(T.right);
        }
    }

    public static void main (String[] args) { //String []args){
        AvlTree<Integer> t = new AvlTree<Integer>();

        t.insert(new Integer(2));
        t.insert(new Integer(1));
        t.insert(new Integer(4));
        t.insert(new Integer(5));
        t.insert(new Integer(9));
        t.insert(new Integer(3));
        t.insert(new Integer(6));
        t.insert(new Integer(7));
        t.insert(new Integer(8));
        t.insert(new Integer(2));
        t.inOrder();
    }
}

class AvlNode<T>{
    protected T element;
    protected AvlNode<T> left;
    protected AvlNode<T> right;
    /**
     *
     */
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
}
