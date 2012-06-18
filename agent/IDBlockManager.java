package agent;

import java.util.*;

public class IDBlockManager {
    private static Map<String,IDBlock> blocks;
    private static IDBlockManager instance = null;

    private IDBlockManager() {
        blocks = new HashMap<String,IDBlock>();
    }

    public static IDBlockManager singleton() {
        synchronized (IDBlockManager.class) {
            if (IDBlockManager.instance == null) {
                IDBlockManager.instance = new IDBlockManager();
            }
        }
        return IDBlockManager.instance;
    }

    public synchronized void addBlock(String table, IDBlock block) {
        IDBlock mBlock = blocks.get(table);
        if (mBlock == null) {
            mBlock = new IDBlock(table);
            blocks.put(table, mBlock);
        }
        mBlock.append(block);
    }

    public synchronized IDBlock consumeBlock(String table, int count) {
        IDBlock r = null;
        IDBlock block = blocks.get(table);
        if (block != null) {
            r = block.consume(count);
        }
        return r;
    }

}
