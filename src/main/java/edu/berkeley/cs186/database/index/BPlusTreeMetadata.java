package edu.berkeley.cs186.database.index;

import edu.berkeley.cs186.database.databox.Type;
import edu.berkeley.cs186.database.databox.TypeId;
import edu.berkeley.cs186.database.table.Record;

// B+树的元数据
/** Metadata about a B+ tree. */
public class BPlusTreeMetadata {
    // Table for which this B+ tree is for
    // 这2个成员没理解,一个翻译为[这个B+树中用于的表],另一个为[B+树用作搜索键的列]
    private final String tableName;

    // Column that this B+ tree uses as a search key
    private final String colName;

    // B+ trees map keys (of some type) to record ids. This is the type of the
    // keys.
    // B+树将键(某种类型)映射到记录id。这是键的类型。
    private final Type keySchema;

    // The order of the tree. Given a tree of order d, its inner nodes store
    // between d and 2d keys and between d+1 and 2d+1 children pointers. Leaf
    // nodes store between d and 2d (key, record id) pairs. Notable exceptions
    // include the root node and leaf nodes that have been deleted from; these
    // may contain fewer than d entries.
    // 树的阶数。
    // 给定阶数为d的树，其内部节点存储d和2d键之间以及d+1和2d+1子指针之间。
    // 叶结点存储在d和2d（键，记录ID）对之间。
    // 值得注意的例外包括已从中删除的根节点和叶节点。
    // 这些可能包含少于d个条目。
    private final int order;

    // The partition that the B+ tree allocates pages from. Every node of the B+ tree
    // is stored on a different page on this partition.
    // B+树分配页的分区。B+树的每个结点
    // 存储在该分区的不同页上。
    private final int partNum;

    // The page number of the root node.
    // 根结点的页码
    private long rootPageNum;

    // The height of this tree.
    // 树的高度
    private int height;

    // 第一种构造方法 这个偏向于方便测试
    public BPlusTreeMetadata(String tableName, String colName, Type keySchema, int order, int partNum,
                             long rootPageNum, int height) {
        this.tableName = tableName;
        this.colName = colName;
        this.keySchema = keySchema;
        this.order = order;
        this.partNum = partNum;
        this.rootPageNum = rootPageNum;
        this.height = height;
    }

    // 第二种构造方法 这个偏向于实际使用
    public BPlusTreeMetadata(Record record) {
        this.tableName = record.getValue(0).getString();
        this.colName = record.getValue(1).getString();
        this.order = record.getValue(2).getInt();
        this.partNum = record.getValue(3).getInt();
        this.rootPageNum = record.getValue(4).getLong();
        this.height = record.getValue(7).getInt();
        int typeIdIndex = record.getValue(5).getInt();
        int typeSize = record.getValue(6).getInt();
        this.keySchema = new Type(TypeId.values()[typeIdIndex], typeSize);
    }

    /**
     * @return a record containing this B+ tree's metadata. Useful for serializing
     * metadata about the tree (see Database#getIndexInfoSchema).
     * 用record的形式,来体现B+树的元数据, 用于序列化关于树的元数据
     */
    public Record toRecord() {
        return new Record(tableName, colName, order, partNum, rootPageNum,
                keySchema.getTypeId().ordinal(), keySchema.getSizeInBytes(),
                height
        );
    }

    // 成员方法
    public String getTableName() {
        return tableName;
    }

    public String getColName() {
        return colName;
    }

    public String getName() {
        return tableName + "," + colName;
    }

    public Type getKeySchema() {
        return keySchema;
    }

    public int getOrder() {
        return order;
    }

    public int getPartNum() {
        return partNum;
    }

    public long getRootPageNum() {
        return rootPageNum;
    }

    void setRootPageNum(long rootPageNum) {
        this.rootPageNum = rootPageNum;
    }

    public int getHeight() {
        return height;
    }

    void incrementHeight() {
        ++height;
    }
}
