package miniDB.index.bptree;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.LinkedList;


/**
 *
 * Class for our Internal nodes
 *
 */
@SuppressWarnings({"WeakerAccess", "unused"})
class TreeInternalNode extends TreeNode {

    private final LinkedList<Long> pointerArray;  // the pointer array

    /**
     * Create an internal node
     *
     * @param nodeType the node type parameter
     * @param pageIndex the index of the page
     */
    TreeInternalNode(TreeNodeType nodeType, long pageIndex) {
        super(nodeType, pageIndex);
        pointerArray = new LinkedList<>();
    }

    void removePointerAt(int index)
        {pointerArray.remove(index);}

    long getPointerAt(int index) {
        return((index < 0 || index >= pointerArray.size()) ? -1 : pointerArray.get(index));}

    long popPointer()
        {return(pointerArray.pop());}

    long removeLastPointer()
        {return(pointerArray.removeLast());}

    void addPointerAt(int index, long val)
        {pointerArray.add(index, val);}

    void addPointerLast(long val)
        {pointerArray.addLast(val);}

    void setPointerAt(int index, long val)
        {pointerArray.set(index, val);}

    int getPointerListSize()
        {return(pointerArray.size());}

    void pushToPointerArray(long val)
        {pointerArray.push(val);}


    /**
     * @param r pointer to *opened* B+ tree file
     * @throws IOException is thrown when an I/O exception is captured.
     */
    @Override
    public void writeNode(RandomAccessFile r, BPlusConfiguration conf)
            throws IOException {

        // update root index in the file
        if(this.isRoot()) {
            r.seek(conf.headerSize-16L);
            r.writeLong(getPageIndex());
        }

        // account for the header page as well.
        r.seek(getPageIndex());

        byte[] buffer = new byte[conf.pageSize];
        ByteBuffer bbuffer = ByteBuffer.wrap(buffer);bbuffer.order(ByteOrder.BIG_ENDIAN);
        // write the node type
        bbuffer.putShort(getPageType());

        // write current capacity
        bbuffer.putInt(getCurrentCapacity());

        // now write Key/Pointer pairs
        for(int i = 0; i < getCurrentCapacity(); i++) {
            bbuffer.putLong(getPointerAt(i));   // Pointer
            conf.writeKey(bbuffer, getKeyAt(i));
        }
        // final pointer.
        bbuffer.putLong(getPointerAt(getCurrentCapacity()));
        r.write(buffer);
    }
}
