package agent;

/*
with fragments from:
org/apache/cassandra/db/commitlog/CommitLog.java
org/apache/cassandra/db/commitlog/CommitLogSegment.java
*/

import java.io.*;
import java.util.*;
import java.util.regex.*;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.io.FileUtils;

import me.prettyprint.cassandra.serializers.*;
import me.prettyprint.cassandra.model.*;
import me.prettyprint.hector.api.*;
import me.prettyprint.cassandra.service.*;
import me.prettyprint.hector.api.beans.*;
import me.prettyprint.hector.api.factory.*;
import me.prettyprint.hector.api.query.*;
import me.prettyprint.hector.api.exceptions.*;
import me.prettyprint.hector.api.ddl.*;

import org.apache.cassandra.thrift.*;


import org.apache.log4j.Logger;

public class CommitLogReader
{
    protected static Logger logger = Logger.getLogger(CommitLogReader.class.getName());

    private int blockSize;
    private int totalProcessed;
    private int deltaProcessed;

    private Map<Integer,String> tableID;

    public CommitLogReader(int blockSize) throws Exception {
        this.blockSize = blockSize;
        totalProcessed = 0;
        deltaProcessed = 0;
        flushBins();

        // read cf definitions to be able to map cf IDs to table names
        tableID = new HashMap<Integer, String>();
        KeyspaceDefinition kd = CassandraManager.singleton().getKeyspaceDefinition();
        List<ColumnFamilyDefinition> cfDefs = kd.getCfDefs();
        for (ColumnFamilyDefinition cf : cfDefs) {
            tableID.put(new Integer(cf.getId()), cf.getName());
        }
    }

    private Map<String,IDBlock> bins = null;

    private void flushBins() {
        Map<String,IDBlock> oldBins = bins;
        bins = new HashMap<String,IDBlock>();
        deltaProcessed = 0;
        if (oldBins == null) {
            
        } else {
            logger.info("*** Flushing mutation bins:");
            for (Map.Entry<String,IDBlock> kv : oldBins.entrySet()) {
                String table = kv.getKey();
                IDBlock ids = kv.getValue();
                logger.info(table + ": " + ids.ids.size());
                IDBlockManager.singleton().addBlock(table, ids);
            }
        }
    }

    private void changed(String table, String key) {
        IDBlock bin = bins.get(table);
        if (bin == null) {
            bin = new IDBlock(table);
            bins.put(table, bin);
        }
        bin.ids.add(key);
        totalProcessed++;
        deltaProcessed++;
        if (deltaProcessed > blockSize) {
            flushBins();
        }
    }

    private static final int MAX_OUTSTANDING_REPLAY_COUNT = 1024;
    private static final int SEGMENT_SIZE = 128*1024*1024;
    
    private static Pattern COMMIT_LOG_FILE_PATTERN = Pattern.compile("CommitLog-(\\d+).log");

    // assume filename is a 'possibleCommitLogFile()'
    private static long idFromFilename(String filename)
    {
        Matcher matcher = COMMIT_LOG_FILE_PATTERN.matcher(filename);
        try {
            if (matcher.matches())
                return Long.valueOf(matcher.group(1));
            else
                return -1L;
        } catch (NumberFormatException e) {
            return -1L;
        }
    }

    private static boolean possibleCommitLogFile(String filename) {
        return COMMIT_LOG_FILE_PATTERN.matcher(filename).matches();
    }

    public void readCommitLogDirectory(final String directory, long rollbackDeltaSeconds) throws IOException
    {
        final long earliest = System.currentTimeMillis() - rollbackDeltaSeconds*1000;
        File[] files = new File(directory).listFiles(new FilenameFilter()
        {
            public boolean accept(File dir, String name)
            {
                if (!CommitLogReader.possibleCommitLogFile(name)) {
                    return false;
                }

                long creation = CommitLogReader.idFromFilename(name);

                // commit log file was created past the earliest time we want to query -> accept it
                if (creation >= earliest) {
                    return true;
                }

                String fullname = directory + "/" + name;
                File file = new File(fullname);
                Long lastModified = file.lastModified();

                // commit log file was modified past the earliest time we want to query -> accept it
                if (lastModified >= earliest) {
                    return true;
                }

                // commit log file is too old -> reject it
                return false;
            }
        });
        if (files == null || files.length == 0) {
            logger.info("No commitlog files found; skipping replay");
            return;
        }

        // is this really needed??
        //Arrays.sort(files, new FileUtils.FileComparator());

        int replayed = recover(files, earliest*1000);
        logger.info("Log scan complete, " + totalProcessed + " accepted mutations");
    }


    private static byte[] readWithShortLength(DataInput dis) throws IOException
    {
        int length = (dis.readByte() & 0xFF) << 8;
        length = length | (dis.readByte() & 0xFF);
        byte[] buff = new byte[length];
        dis.readFully(buff);
        return buff;
    }

    private static byte[] readWithLength(DataInput dis) throws IOException
    {
        int length = dis.readInt();
        byte[] buff = new byte[length];
        dis.readFully(buff);
        return buff;
    }

    private static void skipWithLength(DataInput dis) throws IOException
    {
        int length = dis.readInt();
        dis.skipBytes(length);
    }

    private class Mutation
    {
        public String keyspace;
        public String table;
        public String key;
        public long ts;
    }

/*

keyspace dis.UTF
keySize short
key byte[keySize]

modificationsCount int
    cfID int
    discardIfFalse boolean
    cfIDAgain int
    unk1 int
    unk2 long
    columnCount int
        nameSize short
        name byte[nameSize]

        flags ubyte

        if flags & COUNTER_MASK {
            lastDeleteTimestamp long
            timestamp long
            valueSize int
            value byte[valueSize]
        }

        else if flags & EXPIRATION_MASK {
            ttl int
            expiration int
            timestamp long
            valueSize int
            value byte[valueSize]
        }

        else {
            timestamp long
            valueSize int
            value byte[valueSize]
        }

*/

    public final static int DELETION_MASK       = 0x01;
    public final static int EXPIRATION_MASK     = 0x02;
    public final static int COUNTER_MASK        = 0x04;
    public final static int COUNTER_UPDATE_MASK = 0x08;

    private Mutation deserialize(DataInput dis) throws IOException
    {
        String keyspace = dis.readUTF();
        byte[] keyB = CommitLogReader.readWithShortLength(dis);
        long ts = 0;
        int cfID = 0;

        int modifications = dis.readInt();
        boolean exit = false;

        for (int i = 0; i < modifications && !exit; i++) {
            cfID = dis.readInt();
            boolean discardIfFalse = dis.readBoolean();
            if (!discardIfFalse) {
                continue;
            }
            int cfIDAgain = dis.readInt();
            int unk1 = dis.readInt();
            long unk2 = dis.readLong();

            int columnCount = dis.readInt();
            for (int j = 0; j < columnCount; j++) {

                // check for EOF here??? cassandra does???

                byte[] nameB = CommitLogReader.readWithShortLength(dis);

                int flags = dis.readUnsignedByte();                

                if ((flags & COUNTER_MASK) != 0) {
                    long timestampOfLastDelete = dis.readLong();
                    long colTS = dis.readLong();
                    if (colTS > ts) {
                        ts = colTS;
                    }
                    CommitLogReader.skipWithLength(dis);
                }
                else if ((flags & EXPIRATION_MASK) != 0) {
                    int ttl = dis.readInt();
                    int expiration = dis.readInt();
                    long colTS = dis.readLong();
                    if (colTS > ts) {
                        ts = colTS;
                    }
                    CommitLogReader.skipWithLength(dis);
                }
                else {
                    long colTS = dis.readLong();
                    if (colTS > ts) {
                        ts = colTS;
                    }
                    CommitLogReader.skipWithLength(dis);
                }
            }
        }

        String table = tableID.get(new Integer(cfID));
        Mutation m = null;

        if (table != null && ts > 0) {
            m = new Mutation();
            m.keyspace = keyspace;
            m.table = table;
            m.ts = ts;
            m.key = new String(keyB);
        }

        return m;
    }

    public int recover(File[] clogs, long earliest) throws IOException
    {
        byte[] bytes = new byte[4096];
        Checksum checksum = new CRC32();
        int count = 0;
        int discardedCount = 0;
        int invalidCount = 0;

        for (final File file : clogs)
        {
            final long segment = CommitLogReader.idFromFilename(file.getName());

            RandomAccessFile reader = new RandomAccessFile(file, "r");

            logger.info("Scanning " + file.getAbsolutePath());
            logger.info("Earliest timestamp accepted: " + (new java.util.Date(earliest/1000)) + ", microtime: " + earliest);

            try
            {
                //int replayPosition = 0;

                // maybe do something smart about the replay position to not read the entire file???
                //  dicotomic search with some magic number ident to find out mutations timestamps randomdly in the file?

                //reader.seek(replayPosition);

                /* read the logs populate RowMutation and apply */
                //while (!reader.isEOF())
                while (true)
                {

                    long claimedCRC32;
                    int serializedSize;
                    try
                    {
                        // any of the reads may hit EOF
                        serializedSize = reader.readInt();
                        // RowMutation must be at LEAST 10 bytes:
                        // 3 each for a non-empty Table and Key (including the 2-byte length from
                        // writeUTF/writeWithShortLength) and 4 bytes for column count.
                        // This prevents CRC by being fooled by special-case garbage in the file; see CASSANDRA-2128
                        if (serializedSize < 10)
                            break;
                        long claimedSizeChecksum = reader.readLong();
                        checksum.reset();
                        checksum.update(serializedSize);
                        if (checksum.getValue() != claimedSizeChecksum)
                            break; // entry wasn't synced correctly/fully.  that's ok.

                        if (serializedSize > bytes.length)
                            bytes = new byte[(int) (1.2 * serializedSize)];
                        reader.readFully(bytes, 0, serializedSize);
                        claimedCRC32 = reader.readLong();
                    }
                    catch(EOFException eof)
                    {
                        break; // last CL entry didn't get completely written.  that's ok.
                    }

                    checksum.update(bytes, 0, serializedSize);
                    if (claimedCRC32 != checksum.getValue())
                    {
                        // this entry must not have been fsynced.  probably the rest is bad too,
                        // but just in case there is no harm in trying them (since we still read on an entry boundary)
                        continue;
                    }

                    Mutation m = null;
                    try {
                        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes));
                        m = deserialize(dis);
                    } catch (Exception e) {
                        invalidCount++;
                    }

                    if (m != null) {
                        if (m.ts < earliest) {
                            discardedCount++;
                            continue;
                        }
                        changed(m.table, m.key);
                        count++;

                    } else {
                        discardedCount++;
                    }
                }
            }
            finally
            {
                try {
                    reader.close();
                } catch (Throwable t) { }

                flushBins();
                logger.info("" + count +" accepted mutations, " + discardedCount + " discarded. Finished reading " + file);

            }
        }
        
        return count;
    }

}
