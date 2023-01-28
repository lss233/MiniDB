package miniDB.index.bptree;

import com.lss233.minidb.engine.storage.StorageType;
import com.lss233.minidb.exception.MiniDBException;

import java.util.ArrayList;

public class MainDataConfiguration extends Configuration {
    public int nValidPointerInFreePage;
    public MainDataConfiguration(ArrayList<StorageType> types, ArrayList<Integer> sizes, ArrayList<Integer> colIDs) throws MiniDBException {
        super(0, types, sizes, colIDs);
        // pad keysize to multiples of 16
        int BASE = 16;
        int tmpSize = keySize;
        tmpSize += 8; // space for `RowID`
        if(tmpSize % BASE != 0)
        {
            tmpSize += BASE - (tmpSize % BASE);
        }
        pageSize = Math.max(32, tmpSize);
        nValidPointerInFreePage = pageSize / 8 - 1;
    }
}
