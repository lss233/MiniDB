package miniDB.index;

import com.lss233.minidb.engine.index.NativeMethods;
import com.lss233.minidb.engine.index.Node;

import java.util.ArrayList;
import java.util.LinkedList;

public class BPTree<E extends Comparable<E>> {

    private int leafSize;
    private int internalSize;
    private Node<E> root;
    private int size;
    private NativeMethods<E> nm;
    private LinkedList<Node<E>> buffer;
    private int bufferboolSize;

    public BPTree(int leafSize, int internalSize, int bufferbool) {
        super();
        this.leafSize = leafSize;
        this.internalSize = internalSize;
        this.root = new Node<E>(leafSize, true);
        nm = new NativeMethods<E>();
        buffer = new LinkedList<Node<E>>();
        this.bufferboolSize = bufferbool;
    }

    // ======================================================================
    // ===========================INSERTION==================================
    @SuppressWarnings("unchecked")
    public void insertNode(E key, Object data) {
        // stack to hold parent
        LinkedList<Node<E>> stack = new LinkedList<Node<E>>();
        Node<E> n = root;
        //sezrching fo the element
        while (!n.isLeaf()) {
            stack.push(n);
            // ===================================================
            if (key.compareTo(n.getKeys().get(0)) < 0) {  // if in first pointer
                n = (Node<E>) n.getPointers().get(0);
            } else if (key.compareTo(n.getKeys().get(n.getKeys().size() - 1)) >= 0) {// if in last pointer
                n = (Node<E>) n.getPointers().get(n.getPointers().size() - 1);
            } else {
                for (int i = 0; i < n.getKeys().size() - 1; i++) { // general case
                    if (n.getKeys().size() > 1 && key.compareTo(n.getKeys().get(i)) >= 0 && key.compareTo(n.getKeys().get(i + 1)) < 0) {
                        n = (Node) n.getPointers().get(i + 1);
                        break;
                    }
                }
            }
        }
        // check if the elemnet in the node or not
        for (int i = 0; i < n.getKeys().size(); i++) {
            if (key == n.getKeys().get(i)) {
                return;
            }
        }
        // if node is not full
        if (n.getKeys().size() < leafSize) {
            nm.sortedInsert(key, data, n);
        } else {
            ///    spliting two leaf nodes
            // copying all current node contents in temp node then insert the new element on it
            Node<E> temp = new Node(leafSize, true);
            temp.setKeys(new ArrayList<E>(n.getKeys()));
            temp.setPointers(new ArrayList<Object>(n.getPointers()));
            nm.sortedInsert(key, data, temp);
            Node newNode = new Node(leafSize, true);
            int j = (int) Math.ceil(n.getPointers().size() / (double) 2);
            //take the first half of the temp nde in current node
            n.setKeys(new ArrayList<E>(temp.getKeys().subList(0, j)));
            n.setPointers(new ArrayList<Object>(temp.getPointers().subList(0, j)));
            // next and prev
            if (n.getNext() != null) {
                n.getNext().setPrev(newNode);
            }
            newNode.setNext(n.getNext());
            n.setNext(newNode);
            // copying the rest of temp node in new node
            newNode.setPrev(n);
            newNode.setKeys(new ArrayList<E>(temp.getKeys().subList(j, temp.getKeys().size())));
            newNode.setPointers(new ArrayList<Object>(temp.getPointers().subList(j, temp.getPointers().size())));
            // keeping the key that will be inserting in parent node
            key = temp.getKeys().get(j);
            boolean finished = false;
            do {
                // if the parent is null (root case)
                if (stack.isEmpty()) {
                    root = new Node(internalSize, false);
                    ArrayList<Object> point = new ArrayList<Object>();
                    point.add(n);
                    point.add(newNode);
                    ArrayList<E> keys_ = new ArrayList<E>();
                    keys_.add(key);
                    root.setKeys(keys_);
                    root.setPointers(point);
                    finished = true;
                } else {
                    // if there's parent
                    n = stack.pop();
                    // if there's no need for splitting internal
                    if (n.getKeys().size() < internalSize) {
                        nm.sortedInsertInternal(key, newNode, n);
                        finished = true;
                    } else {
                        /* splitting two internal nodes by copying them into new node and insert
                        new elemnet in the temp node then divide it betwwen current node and new node
                         */
                        temp.setLeaf(false);
                        temp.setKeys(new ArrayList<E>(n.getKeys()));
                        temp.setPointers(new ArrayList<Object>(n.getPointers()));

                        nm.sortedInsertInternal(key, newNode, temp);
                        newNode = new Node(internalSize, false);
                        j = (int) Math.ceil(temp.getPointers().size() / (double) 2);

                        n.setKeys(new ArrayList<E>(temp.getKeys().subList(0, j - 1)));
                        n.setPointers(new ArrayList<Object>(temp.getPointers().subList(0, j)));
                        if (n.getNext() != null) {
                            n.getNext().setPrev(newNode);
                        }
                        newNode.setNext(n.getNext());
                        n.setNext(newNode);
                        newNode.setPrev(n);
                        newNode.setKeys(new ArrayList<E>(temp.getKeys().subList(j, temp.getKeys().size())));
                        newNode.setPointers(new ArrayList<Object>(temp.getPointers().subList(j, temp.getPointers().size())));

                        key = temp.getKeys().get(j - 1);
                    }
                }
            } while (!finished);
        }
    }
//======================================BULK INSERTION=================================================

    @SuppressWarnings("unchecked")
    public void insertBulk(ArrayList<E> keys, ArrayList<Object> records) {
        E key;
        boolean firstInsert = true;
        int first = 0;
        int second = 0;
        for (int i = 0; i < Math.ceil(keys.size() / (double) leafSize); i++) {
            // stack to hold parent
            LinkedList<Node<E>> stack = new LinkedList<Node<E>>();
            Node<E> n = root;
            first = second;
            second = second + leafSize;
            if (second > keys.size()) {
                second = keys.size();
            }
            ArrayList<E> newKeys = new ArrayList<E>(keys.subList(first, second));
            ArrayList<Object> newRecords = new ArrayList<Object>(records.subList(first, second));
            // getting the right most elemnet in the tree
            while (!n.isLeaf()) {
                stack.push(n);
                n = (Node<E>) n.getPointers().get(n.getPointers().size() - 1);
            }
            // if its the first insert
            if (firstInsert) {
                root.setKeys(newKeys);
                root.setPointers(newRecords);
                firstInsert = false;
            } else {
                //    spliting two leaf nodes
                // copying all current node contents in temp node then insert the new element on it
                Node<E> temp = new Node(leafSize, true);
                temp.setKeys(new ArrayList<E>(n.getKeys()));
                temp.setPointers(new ArrayList<Object>(n.getPointers()));
                temp.getKeys().addAll(newKeys);
                temp.getPointers().addAll(newRecords);
                Node newNode = new Node(leafSize, true);
                int j = (int) Math.ceil(temp.getPointers().size() / (double) 2);
                //take the first half of the temp nde in current node
                n.setKeys(new ArrayList<E>(temp.getKeys().subList(0, j)));
                n.setPointers(new ArrayList<Object>(temp.getPointers().subList(0, j)));
                if (n.getNext() != null) {
                    n.getNext().setPrev(newNode);
                }
                newNode.setNext(n.getNext());
                n.setNext(newNode);
                // copying other elements
                newNode.setPrev(n);
                newNode.setKeys(new ArrayList<E>(temp.getKeys().subList(j, temp.getKeys().size())));
                newNode.setPointers(new ArrayList<Object>(temp.getPointers().subList(j, temp.getPointers().size())));
                key = temp.getKeys().get(j);
                boolean finished = false;
                // keeping the key that will be inserting in parent node
                do {
                    // if the parent is null (root case)
                    if (stack.isEmpty()) {
                        root = new Node(internalSize, false);
                        ArrayList<Object> point = new ArrayList<Object>();
                        point.add(n);
                        point.add(newNode);
                        ArrayList<E> keys_ = new ArrayList<E>();
                        keys_.add(key);
                        root.setKeys(keys_);
                        root.setPointers(point);
                        finished = true;
                    } else {
                        // if there's parent
                        n = stack.pop();
                        // if there's no need for splitting internal
                        if (n.getKeys().size() < internalSize) {
                            nm.sortedInsertInternal(key, newNode, n);
                            finished = true;
                        } else {
                            /* splitting two internal nodes by copying them into new node and insert
                            new elemnet in the temp node then divide it betwwen current node and new node
                             */
                            temp.setLeaf(false);
                            temp.setKeys(new ArrayList<E>(n.getKeys()));
                            temp.setPointers(new ArrayList<Object>(n.getPointers()));
                            nm.sortedInsertInternal(key, newNode, temp);
                            newNode = new Node(internalSize, false);
                            j = (int) Math.ceil(temp.getPointers().size() / (double) 2);
                            n.setKeys(new ArrayList<E>(temp.getKeys().subList(0, j - 1)));
                            n.setPointers(new ArrayList<Object>(temp.getPointers().subList(0, j)));
                            if (n.getNext() != null) {
                                n.getNext().setPrev(newNode);
                            }
                            newNode.setNext(n.getNext());
                            n.setNext(newNode);
                            newNode.setPrev(n);
                            newNode.setKeys(new ArrayList<E>(temp.getKeys().subList(j, temp.getKeys().size())));
                            newNode.setPointers(new ArrayList<Object>(temp.getPointers().subList(j, temp.getPointers().size())));
                            key = temp.getKeys().get(j - 1);
                        }
                    }
                } while (!finished);
            }
        }
    }

    // ======================================================================
    // =============================SEARCHING================================
    @SuppressWarnings("unchecked")
    public Node<E> search(E key) {
        // secrhing in buffer array to check if the required
        // element on it or not
        for (int i = 0; i < buffer.size(); i++) {
            ArrayList<E> find = buffer.get(i).getKeys();
            if (find.contains(key)) {
                return buffer.get(i);
            }
        }
        // if the elemnet isn't in buffer bool
        Node<E> n = root;
        while (!n.isLeaf()) {
            //sezrching fo the element
            if (key.compareTo(n.getKeys().get(0)) < 0) {// if in the first pointer
                n = (Node<E>) n.getPointers().get(0);
            } else if (key.compareTo(n.getKeys().get(n.getKeys().size() - 1)) >= 0) {// if in the last pointer
                n = (Node<E>) n.getPointers().get(n.getPointers().size() - 1);
            } else {
                for (int i = 0; i < n.getKeys().size() - 1; i++) {
                    if (n.getKeys().size() > 1 && key.compareTo(n.getKeys().get(i)) >= 0 && key.compareTo(n.getKeys().get(i + 1)) < 0) {// general case
                        n = (Node) n.getPointers().get(i + 1);
                        break;
                    }
                }
            }
        }
        // adding new node to buffre bool
        for (int i = 0; i < n.getKeys().size(); i++) {
            if (key.compareTo(n.getKeys().get(i)) == 0) {
                if (buffer.size() == bufferboolSize) {
                    buffer.removeFirst();
                    buffer.add(n);
                } else {
                    buffer.add(n);
                }
                return n;
            }
        }
        return null;
    }

    // ======================================================================
    // ==============================DELETION================================
    @SuppressWarnings("unchecked")
    public void delete(E key) {
        LinkedList<Node<E>> stack = new LinkedList<Node<E>>();
        Node<E> n = root;
        //secrching for the required node
        while (!n.isLeaf()) {
            stack.push(n);
            // ===================================================
            if (key.compareTo(n.getKeys().get(0)) < 0) {
                n = (Node<E>) n.getPointers().get(0);
            } else if (key.compareTo(n.getKeys().get(n.getKeys().size() - 1)) >= 0) {
                n = (Node) n.getPointers().get(n.getPointers().size() - 1);
            } else {
                for (int i = 0; i < n.getKeys().size(); i++) {
                    if (key.compareTo(n.getKeys().get(i)) >= 0 && key.compareTo(n.getKeys().get(i + 1)) < 0) {
                        n = (Node) n.getPointers().get(i + 1);
                        break;
                    }
                }
            }
        }
        // END OF WHILE
        boolean flag = false;
        for (int i = 0; i < n.getKeys().size(); i++) {
            if (n == root && key == n.getKeys().get(i)) {
                nm.deleteNode(n, key);
                return;
            } else if (key == n.getKeys().get(i)) {
                flag = true;
                break;
            }
        }
        //searching to determine if the element is found in leaf node or not
        if (flag) {
            //if the node isn't under flow
            if (n.getKeys().size() - 1 >= Math.ceil(leafSize / 2.0)) {
                nm.deleteNode(n, key);
                Node<E> parent = stack.peek();
                for (int i = 0; i < parent.getKeys().size(); i++) {
                    if (key.compareTo(parent.getKeys().get(i)) == 0) {
                        parent.getKeys().set(i, n.getKeys().get(0));
                        break;
                    }
                }
            } else {
                // if node is in underflow
                Node<E> parent = stack.peek();
                // determin if the next node is from the same parent or not to borrow from it
                int deter = nm.sameParent(n, stack.peek(), leafSize);
                // if next from the same parent
                if (deter == 1) {
                    // delete the node
                    nm.deleteNode(n, key);
                    // borrow from the next leaf node
                    E element = n.getNext().getKeys().remove(0);
                    Object obj = n.getNext().getPointers().remove(0);
                    n.getKeys().add(element);
                    n.getPointers().add(obj);
                    for (int i = 0; i < parent.getKeys().size(); i++) {
                        if (element.compareTo(parent.getKeys().get(i)) == 0) {
                            parent.getKeys().set(i, n.getNext().getKeys().get(0));
                            break;
                        }
                    }
                    for (int i = 0; i < parent.getKeys().size(); i++) {
                        if (key.compareTo(parent.getKeys().get(i)) == 0) {
                            parent.getKeys().set(i, n.getKeys().get(0));
                            break;
                        }
                    }
                    return;
                } else if (deter == 2) {
                    // borrow from the previous node
                    nm.deleteNode(n, key);
                    E element = n.getPrev().getKeys().remove(n.getPrev().getKeys().size() - 1);
                    Object obj = n.getPrev().getPointers().remove(n.getPrev().getPointers().size() - 1);
                    n.getKeys().add(0, element);
                    n.getPointers().add(0, obj);
                    for (int i = 0; i < parent.getKeys().size(); i++) {
                        if (element.compareTo(parent.getKeys().get(i)) == 0) {
                            parent.getKeys().set(i, n.getPrev().getKeys().get(n.getPrev().getKeys().size() - 1));
                            break;
                        }
                    }
                    for (int i = 0; i < parent.getKeys().size(); i++) {
                        if (key.compareTo(parent.getKeys().get(i)) == 0) {
                            parent.getKeys().set(i, n.getKeys().get(0));
                            break;
                        }
                    }
                    return;
                } else {
                    // there will be merging for internal nodes
                    boolean prevB = true;
                    if (key == n.getKeys().get(0)) {
                        prevB = false;
                    }

                    nm.deleteNode(n, key);
                    int tempKey = 0;
                    int tempPointer = 0;
                    // if the merging will be with the next node
                    // then copying all elemnts of the current node in the next node
                    // dalete the first element from the next node in the parent node
                    if (nm.sameParent2(parent, n)) {
                        Node<E> next = n.getNext();
                        if (n.getPrev() != null) {
                            n.getPrev().setNext(next);
                        }
                        if (next != null) {
                            next.setPrev(n.getPrev());
                        }
                        n.setNext(null);
                        n.setPrev(null);
                        next.getKeys().addAll(0, n.getKeys());
                        next.getPointers().addAll(0, n.getPointers());
                        for (int i = 0; i < parent.getKeys().size(); i++) {
                            if (next.getKeys().get(n.getKeys().size()).compareTo(parent.getKeys().get(i)) == 0) {
                                tempKey = i;
                                tempPointer = i;
                                break;
                            }
                        }
                        if (tempKey > 0 && parent.getKeys().get(tempKey - 1) == key) {
                            parent.getKeys().set(tempKey - 1, next.getKeys().get(0));
                        }
                    } else {
                        // if the merging will be with the prev node
                        // then copying all elemnts of the node in the prev node
                        // dalete the first element from the current node in the parent node
                        Node<E> prev = n.getPrev();
                        if (prev != null) {
                            prev.setNext(n.getNext());
                        }
                        if (n.getNext() != null) {
                            n.getNext().setPrev(prev);
                        }
                        n.setNext(null);
                        n.setPrev(null);
                        prev.getKeys().addAll(n.getKeys());
                        prev.getPointers().addAll(n.getPointers());
                        if (prevB) {
                            for (int i = 0; i < parent.getKeys().size(); i++) {
                                if (n.getKeys().get(0).compareTo(parent.getKeys().get(i)) == 0) {
                                    tempKey = i;
                                    tempPointer = i + 1;
                                    break;
                                }
                            }
                        } else {
                            for (int i = 0; i < parent.getKeys().size(); i++) {
                                if (key.compareTo(parent.getKeys().get(i)) == 0) {
                                    tempKey = i;
                                    tempPointer = i + 1;
                                    break;
                                }
                            }
                        }
                    }
                    boolean finished = false;
                    do {
                        // if we get the root
                        if (stack.isEmpty()) {
                            root.getKeys().remove(tempKey);
                            root.getPointers().remove(tempPointer);
                            finished = true;
                        } else {
                            n = stack.pop();
                            //try borrowing from the cebeling
                            if (n.getKeys().size() - 1 >= 1) {
                                n.getKeys().remove(tempKey);
                                n.getPointers().remove(tempPointer);
                                finished = true;
                            } else {
                                // if the root have one cebeling
                                // the tree level will decrease
                                if (n == root) {
                                    n.getKeys().remove(tempKey);
                                    n.getPointers().remove(tempPointer);
                                    if (n.getPointers().size() == 1) {
                                        root = (Node<E>) n.getPointers().get(0);
                                    }
                                    finished = true;
                                } else {
                                    n.getKeys().remove(tempKey);
                                    n.getPointers().remove(tempPointer);
                                    deter = nm.nexOrprev(n, stack.peek(), internalSize);
                                    parent = stack.peek();
                                    // borrowing from next internal node
                                    if (deter == 1) {
                                        int index = -1;
                                        for (int i = 0; i < parent.getPointers().size(); i++) {
                                            if (parent.getPointers().get(i) == n.getNext()) {
                                                index = i;
                                                break;
                                            }
                                        }
                                        E tempkey = parent.getKeys().remove(index - 1);
                                        n.getKeys().add(tempkey);
                                        Node<E> tempNext = (Node<E>) n.getNext().getPointers().remove(0);
                                        E nextKey = n.getNext().getKeys().remove(0);
                                        n.getPointers().add(tempNext);
                                        parent.getKeys().add(index - 1, nextKey);
                                        finished = true;
                                        // boorwing form prev internal node
                                    } else if (deter == 2) {
                                        int index = -1;
                                        for (int i = 0; i < parent.getPointers().size(); i++) {
                                            if (parent.getPointers().get(i) == n) {
                                                index = i;
                                                break;
                                            }
                                        }
                                        E tempkey = parent.getKeys().remove(index - 1);
                                        n.getKeys().add(0, tempkey);
                                        Node<E> tempPrev = (Node<E>) n.getPrev().getPointers().remove(n.getPrev().getPointers().size() - 1);
                                        E prevKey = n.getPrev().getKeys().remove(n.getPrev().getKeys().size() - 1);
                                        n.getPointers().add(0, tempPrev);
                                        parent.getKeys().add(index - 1, prevKey);
                                        finished = true;
                                    } else {
                                        // mergae two internal nodes
                                        if (nm.sameParent2(parent, n)) {
                                            for (int i = 0; i < parent.getPointers().size(); i++) {
                                                if (n == parent.getPointers().get(i)) {
                                                    tempKey = i;
                                                    tempPointer = i;
                                                    break;
                                                }
                                            }
                                            Node<E> next = n.getNext();
                                            if (n.getPrev() != null) {
                                                n.getPrev().setNext(next);
                                            }
                                            if (next != null) {
                                                next.setPrev(n.getPrev());
                                            }
                                            next.getKeys().add(0, parent.getKeys().get(tempKey));
                                            next.getKeys().addAll(0, n.getKeys());
                                            next.getPointers().addAll(0, n.getPointers());

                                        } else {
                                            for (int i = 0; i < parent.getPointers().size(); i++) {
                                                if (n == parent.getPointers().get(i)) {
                                                    tempKey = i - 1;
                                                    tempPointer = i;
                                                    break;
                                                }
                                            }
                                            Node<E> prev = n.getPrev();
                                            if (prev != null) {
                                                prev.setNext(n.getNext());
                                            }
                                            if (n.getNext() != null) {
                                                n.getNext().setPrev(prev);
                                            }
                                            prev.getKeys().add(parent.getKeys().get(tempKey));
                                            prev.getKeys().addAll(n.getKeys());
                                            prev.getPointers().addAll(n.getPointers());
                                        }
                                    }
                                }
                            }
                        }
                    } while (!finished);

                }
            }
        } else { // if the elemnet isn't found
            return;
        }
    }

    // ======================================================================
// ===========================GETTERS AND SETTERS========================
    public int getLeafSize() {
        return leafSize;
    }

    public void setLeafSize(int leafSize) {
        this.leafSize = leafSize;
    }

    public int getInternalSize() {
        return internalSize;
    }

    public void setInternalSize(int internalSize) {
        this.internalSize = internalSize;
    }

    public Node<E> getRoot() {
        return root;
    }

    public void setRoot(Node<E> root) {
        this.root = root;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;


    }

    @SuppressWarnings("unchecked")
    public String print() {
        String s = "";
        LinkedList<Node<E>> view = new LinkedList<Node<E>>();
        view.add(root);
        while (!view.isEmpty()) {
            Node<E> e = view.pop();
            for (int i = 0; i
                    < e.getKeys().size(); i++) {
                s += (e.getKeys().get(i) + " ");
            }
            for (int i = 0; i < e.getPointers().size(); i++) {
                try {
                    view.add((Node<E>) e.getPointers().get(i));
                } catch (Exception e1) {
                }
            }
            s += "\n";
        }
        return s;
    }

    public static void main(String[] args) {

        BPTree<Integer> tr = new BPTree<Integer>(6, 4,3);
        int temp = 0;
//        for (int i = 0; i < 100; i++) {
//            tr.insertNode((int) (Math.random() * 100), i + "");
//            tr.print();
//            System.out.println("==========================================");
//         }
        for (int i = 0; i < 1000; i++) {
            tr.insertNode(i,String.valueOf(i));
        }

        for (Object o : tr.search(300).getPointers()) {
            System.out.println("here:"+o.toString());
        }
//        tr.insertNode(37, "22");
//        tr.insertNode(38, "17");
//        tr.insertNode(100, "19");
////        tr.insertNode(20, "20");
////        tr.insertNode(28, "28");
////        tr.insertNode(29, "29");
////        tr.insertNode(40, "40");
////        tr.insertNode(47, "47");
////        tr.insertNode(23, "23");
////        tr.insertNode(24, "24");
////        tr.insertNode(100, "100");
////        tr.insertNode(77, "77");
////        tr.insertNode(60, "60");
////        tr.insertNode(36, "36");
////        tr.insertNode(37, "37");
//        tr.delete(100);
//        tr.delete(38);
//        tr.delete(37);
////        tr.delete(28);
////        tr.delete(15);
////        tr.delete(19);
////        tr.delete(27);
////        tr.delete(47);
////        tr.delete(40);
////        tr.delete(23);
////        tr.delete(17);
////        tr.delete(5);
////        tr.delete(3);
////        tr.insertNode(45, "45");
////        tr.delete(24);
////        tr.delete(29);
////        tr.delete(30);
////        tr.delete(36);
////        tr.delete(45);
////        tr.delete(60);
//        tr.insertNode(33, "33");
////        tr.delete(77);
////        tr.delete(100);
//        tr.delete(100);
//        System.out.println(tr.print());

    }
}
