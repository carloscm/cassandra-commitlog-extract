package agent;

import java.io.*;
import java.util.*;

public class FileWriter {

    private String fileName;
    private String extension;
    private long fragmentSize;

    private int fragment;
    private long bytes;
    private OutputStream out;

    public FileWriter(String fileName, String extension, long fragmentSize) throws Exception {
        this.fileName = fileName;
        this.extension = extension;
        this.fragmentSize = fragmentSize;
        fragment = 0;
        bytes = 0;
        out = null;
        newFragment();
    }

    private void newFragment() throws Exception {
        close();
        fragment++;
        out = new FileOutputStream(fileName + "-" + String.format("%05d", fragment) + "." + extension);
        bytes = 0;
    }

    private void prepareFragment() throws Exception {
        if (bytes > fragmentSize)
            newFragment();
    }

    public void close() throws Exception {
        if (out != null)
            out.close();
        out = null;
    }

    public void write(String s) throws Exception {
        prepareFragment();
        byte[] b = s.getBytes();
        out.write(b);
        bytes += b.length;
    }

}
