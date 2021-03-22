package edu.berkeley.cs186.database.databox;

/**
 * 这里相当于是我所有类型的枚举
 * 在后续的类型操作的时候方便调用
 */
public enum TypeId {
    BOOL,
    INT,
    FLOAT,
    STRING,
    LONG,
    BYTE_ARRAY;

    private static final TypeId[] values = TypeId.values();

    public static TypeId fromInt(int x) {
        if (x < 0 || x >= values.length) {
            String err = String.format("Unknown TypeId ordinal %d.", x);
            throw new IllegalArgumentException(err);
        }
        return values[x];
    }
}
