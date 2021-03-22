package edu.berkeley.cs186.database.index;

/**
 * 异常的封装
 */
@SuppressWarnings("serial")
public class BPlusTreeException extends RuntimeException {
    public BPlusTreeException(String message) {
        super(message);
    }
}
