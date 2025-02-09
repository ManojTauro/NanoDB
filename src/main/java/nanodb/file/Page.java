package nanodb.file;

import nanodb.constants.NanoDBConstants;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class Page {
    private final ByteBuffer buffer;
    public static final Charset CHARSET = StandardCharsets.US_ASCII;

    public Page() {
        this.buffer = ByteBuffer.allocateDirect(NanoDBConstants.PAGE_SIZE);
    }

    public Page(byte[] b) {
        buffer = ByteBuffer.wrap(b);
    }

    public int getInt(int offset) {
        return buffer.getInt(offset);
    }

    public void putInt(int value, int offset) {
        buffer.putInt(value, offset);
    }

    public byte[] getBytes(int offset) {
        buffer.position(offset);
        byte[] ans = new byte[buffer.getInt()];
        buffer.get(ans);

        return ans;
    }

    public void putBytes(int offset, byte[] b) {
        buffer.position(offset);
        buffer.putInt(b.length);
        buffer.put(b);
    }

    public String getString(int offset) {
        byte[] b = getBytes(offset);
        return new String(b, CHARSET);
    }

    public void putString(int offset, String value) {
        byte[] b = value.getBytes(CHARSET);
        putBytes(offset, b);
    }

    ByteBuffer contents() {
        buffer.position(0);
        return buffer;
    }

    /**
     * @param len - Number of characters in a String to be stored in a byte array
     * @return - Maximum bytes needed to store the string with length 'len'
     */
    public static int maxLength(int len) {
        float bytesPerChar = CHARSET.newEncoder().maxBytesPerChar();
        return Integer.BYTES + (len * (int) bytesPerChar);
    }
}