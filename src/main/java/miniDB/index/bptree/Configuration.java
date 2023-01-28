package miniDB.index.bptree;

import com.lss233.minidb.engine.storage.StorageType;
import com.lss233.minidb.exception.MiniDBException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.function.BiFunction;

public class Configuration {
    public int pageSize;           // page size (in bytes)
    public int keySize;            // key size (in bytes)
    public ArrayList<StorageType> types; // Keys may contain multiple columns. `types` tracks the type for each column
    // use Integer/Float etc for primitive types
    public ArrayList<Integer> sizes; // size of each key type (in bytes)
    public ArrayList<Integer> colIDs; // ID of each column
    public ArrayList<Integer> strColLocalId; // ID of string columns (in local Id, not the id from the whole table)

    public Configuration(int pageSize, ArrayList<StorageType> types, ArrayList<Integer> sizes, ArrayList<Integer> colIDs) throws MiniDBException {
        this.colIDs = colIDs;
        this.types = types;
        for(StorageType each : types) {
            if(each != StorageType.Int && each != StorageType.Long && each != StorageType.Float && each != StorageType.Double && each != StorageType.String) {
                throw new MiniDBException(String.format(MiniDBException.Companion.getUnknownColumnType(), each));
            }
        }
        strColLocalId = new ArrayList<>();
        for(int i = 0; i < types.size(); ++i) {
            if(types.get(i) == StorageType.String) {
                strColLocalId.add(i);
            }
        }
        this.sizes = sizes;
        keySize = 0;
        for(int each : sizes) {
            keySize += each;
        }
        this.pageSize = pageSize;   // page size (in bytes)
    }

    /*compare function with short-cut evaluation.**/
    private boolean compare(ArrayList<Object> key1, ArrayList<Object> key2, BiFunction<Integer, Integer, Boolean> func, boolean finalValue) {
        for(int j = 0; j < types.size(); ++j) {
            if(types.get(j) == StorageType.Int) {
                int ans = Integer.compare((Integer) key1.get(j), (Integer) key2.get(j));
                if(ans == 0) {
                    continue;
                }
                return func.apply(ans, 0);
            }else if(types.get(j) == StorageType.Long) {
                int ans = Long.compare((Long) key1.get(j), (Long) key2.get(j));
                if(ans == 0) {
                    continue;
                }
                return func.apply(ans, 0);
            } else if(types.get(j) == StorageType.Float) {
                int ans = Float.compare((Float) key1.get(j), (Float) key2.get(j));
                if(ans == 0) {
                    continue;
                }
                return func.apply(ans, 0);
            } else if(types.get(j) == StorageType.Double) {
                int ans = Double.compare((Double) key1.get(j), (Double) key2.get(j));
                if(ans == 0) {
                    continue;
                }
                return func.apply(ans, 0);
            } else if(types.get(j) == StorageType.String) {
                int ans = ((String) key1.get(j)).compareTo((String) key2.get(j));
                if(ans == 0) {
                    continue;
                }
                return func.apply(ans, 0);
            }
        }
        // every objects are equal
        return finalValue;
    }

    // > op
    public boolean gt(ArrayList<Object> key1, ArrayList<Object> key2)
    {
        return compare(key1, key2, (Integer x, Integer y) -> x > y, false);
    }

    // >= op
    public boolean ge(ArrayList<Object> key1, ArrayList<Object> key2)
    {
        return compare(key1, key2, (Integer x, Integer y) -> x > y, true);
    }

    // < op
    public boolean lt(ArrayList<Object> key1, ArrayList<Object> key2)
    {
        return compare(key1, key2, (Integer x, Integer y) -> x < y, false);
    }
    // <= op
    public boolean le(ArrayList<Object> key1, ArrayList<Object> key2)
    {
        return compare(key1, key2, (Integer x, Integer y) -> x < y, true);
    }

    // != op
    public boolean neq(ArrayList<Object> key1, ArrayList<Object> key2)
    {
        return compare(key1, key2, (Integer x, Integer y) -> !x.equals(y), false);
    }

    // == op
    public boolean eq(ArrayList<Object> key1, ArrayList<Object> key2)
    {
        return !neq(key1, key2);
    }


    public void writeKey(ByteBuffer r, ArrayList<Object> key) {
        padKey(key);
        for(int j = 0; j < types.size(); ++j) {
            if(types.get(j) == StorageType.Int) {
                r.putInt((Integer) key.get(j));
            } else if(types.get(j) == StorageType.Long) {
                r.putLong((Long) key.get(j));
            } else if(types.get(j) == StorageType.Float) {
                r.putFloat((Float) key.get(j));
            } else if(types.get(j) == StorageType.Double) {
                r.putDouble((Double) key.get(j));
            } else if(types.get(j) == StorageType.String) {
                r.put(((String) key.get(j)).getBytes(StandardCharsets.UTF_8));
            }
        }
    }

    public ArrayList<Object> readKey(ByteBuffer r) throws IOException
    {
        ArrayList<Object> key = new ArrayList<>(Arrays.asList(new Object[types.size()]));
        for(int j = 0; j < types.size(); ++j)
        {
            if(types.get(j) == StorageType.Int)
            {
                key.set(j, r.getInt());
            }else if(types.get(j) == StorageType.Long)
            {
                key.set(j, r.getLong());
            }else if(types.get(j) == StorageType.Float)
            {
                key.set(j, r.getFloat());
            }else if(types.get(j) == StorageType.Double)
            {
                key.set(j, r.getDouble());
            }else if(types.get(j) == StorageType.String)
            {
                //TODO possible not efficient. buffer is copied into the string?
                byte[] buffer = new byte[sizes.get(j)];
                r.get(buffer, 0, sizes.get(j));
                key.set(j, new String(buffer, StandardCharsets.UTF_8));
            }
        }
        return key;
    }

    public void printKey(ArrayList<Object> key)
    {
        System.out.println(keyToString(key));
    }

    public String keyToString(ArrayList<Object> key)
    {
        StringBuilder ans = new StringBuilder();
        ans.append("[");
        for(int i = 0; i < types.size(); ++i)
        {
            if(types.get(i) == StorageType.Int)
            {
                ans.append(key.get(i));
                ans.append(' ');
            }else if(types.get(i) == StorageType.Long)
            {
                ans.append(key.get(i));
                ans.append(' ');
            }else if(types.get(i) == StorageType.Float)
            {
                ans.append(key.get(i));
                ans.append(' ');
            }else if(types.get(i) == StorageType.Double)
            {
                ans.append(key.get(i));
                ans.append(' ');
            }else if(types.get(i) == StorageType.String)
            {
                ans.append((String) key.get(i));
                ans.append(' ');
            }
        }
        ans.append("]");
        return ans.toString();
    }



    public static String padString(String arg, int nBytes) throws MiniDBException
    {
        int size = arg.getBytes(StandardCharsets.UTF_8).length;
        if(size > nBytes) {
            throw new MiniDBException(String.format(MiniDBException.StringLengthOverflow, nBytes, arg, size));
        }
        if(size == nBytes) {
            return arg;
        }
        return arg + new String(new char[nBytes - size]).replace('\0', ' ');
    }

    public ArrayList<Object> padKey(ArrayList<Object> key) throws MiniDBException
    {
        for (Integer i : strColLocalId)
        {
            key.set(i, padString((String) key.get(i), sizes.get(i)));
        }
        return key;
    }
}
