package edu.berkeley.cs186.database.index;

import java.util.Iterator;
import java.util.Optional;

import edu.berkeley.cs186.database.common.Buffer;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.concurrency.LockContext;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.memory.BufferManager;
import edu.berkeley.cs186.database.memory.Page;
import edu.berkeley.cs186.database.table.RecordId;

/**
 * B+树 节点 抽象类 定义了节点的一些核心API
 *
 *
 * An inner node or a leaf node. See InnerNode and LeafNode for more
 * information.
 */
abstract class BPlusNode {
    // Core API ////////////////////////////////////////////////////////////////
    /**
     * n.get(k) returns the leaf node on which k may reside when queried from n.
     * For example, consider the following B+ tree (for brevity, only keys are
     * shown; record ids are omitted).
     *
     * n.get(k)从n查询时返回k所在的叶节点。
     * 例如，考虑下面的B+树(为了简洁起见，只显示键，忽略记录id)。
     *
     *                               inner
     *                               +----+----+----+----+
     *                               | 10 | 20 |    |    |
     *                               +----+----+----+----+
     *                              /     |     \
     *                         ____/      |      \____
     *                        /           |           \
     *   +----+----+----+----+  +----+----+----+----+  +----+----+----+----+
     *   |  1 |  2 |  3 |    |->| 11 | 12 | 13 |    |->| 21 | 22 | 23 |    |
     *   +----+----+----+----+  +----+----+----+----+  +----+----+----+----+
     *   leaf0                  leaf1                  leaf2
     *
     * inner.get(x) should return 内部结点执行get时应该有以下的几种返回
     *
     *   - leaf0 when x < 10, 叶结点0应该返回 x < 10 的键,下面的以此类推
     *   - leaf1 when 10 <= x < 20, and
     *   - leaf2 when x >= 20.
     *
     * Note that inner.get(4) would return leaf0 even though leaf0 doesn't
     * actually contain 4.
     * 注意，inner.get(4)将返回leaf0，即使leaf0实际上并不包含4。
     */
    public abstract LeafNode get(DataBox key);

    /**
     * n.getLeftmostLeaf() returns the leftmost leaf in the subtree rooted by n.
     * In the example above, inner.getLeftmostLeaf() would return leaf0, and
     * leaf1.getLeftmostLeaf() would return leaf1.
     * getleftmostleaf() 返回以n为根的子树中最左边的叶子。
     * 在上面的示例中
     * inner.getLeftmostLeaf()将返回leaf0
     * leaf1.getLeftmostLeaf()将返回leaf1。
     */
    public abstract LeafNode getLeftmostLeaf();

    /**
     * n.put(k, r) inserts the pair (k, r) into the subtree rooted by n. There
     * are two cases to consider:
     *
     *   Case 1: If inserting the pair (k, r) does NOT cause n to overflow, then
     *           Optional.empty() is returned.
     *   第一种情况: 如果插入数据对 pair (k, r) 没有使n溢出, 则该插入方法返回一个Optional.empty()
     *
     *   Case 2: If inserting the pair (k, r) does cause the node n to overflow,
     *           then n is split into a left and right node (described more
     *           below) and a pair (split_key, right_node_page_num) is returned
     *           where right_node_page_num is the page number of the newly
     *           created right node, and the value of split_key depends on
     *           whether n is an inner node or a leaf node (described more below).
     *
     *   第二种情况: 如果插入数据对 pair (k, r) 导致n溢出, 然后 n 需要分裂为左右节点
     *              返回一个pair，其中right_node_page_num是新创建的右结点的页码，split_key的值取决于n是内部结点还是叶结点
     *
     * Now we explain how to split nodes and which split keys to return. Let's
     * take a look at an example. Consider inserting the key 4 into the example
     * tree above. No nodes overflow (i.e. we always hit case 1). The tree then
     * looks like this:
     *
     * 现在我们解释如何分割节点和要返回的拆分键, 让我们来撸个例子. 根据上面注释的例子,我们来插入一个 4
     * 因为leaf0中还有空间,所以没有结点溢出(也就是说,我们命中了第一种情况), 然后插入完后的树就是下面这个样子;
     *
     *                               inner
     *                               +----+----+----+----+
     *                               | 10 | 20 |    |    |
     *                               +----+----+----+----+
     *                              /     |     \
     *                         ____/      |      \____
     *                        /           |           \
     *   +----+----+----+----+  +----+----+----+----+  +----+----+----+----+
     *   |  1 |  2 |  3 |  4 |->| 11 | 12 | 13 |    |->| 21 | 22 | 23 |    |
     *   +----+----+----+----+  +----+----+----+----+  +----+----+----+----+
     *   leaf0                  leaf1                  leaf2
     *
     * Now let's insert key 5 into the tree. Now, leaf0 overflows and creates a
     * new right sibling leaf3. d entries remain in the left node; d + 1 entries
     * are moved to the right node. DO NOT REDISTRIBUTE ENTRIES ANY OTHER WAY. In
     * our example, leaf0 and leaf3 would look like this:
     *
     * 接下来我们继续插入一个5到树中, 因为leaf0原来有4个元素,添加一个新元素后达到溢出条件.开始进行以下操作:
     * 创建一个新的兄弟叶结点leaf3. 阶数d个关键字存在左结点(leaf0结点),阶数d+1个关键字移动到右叶子结点;
     * 注意: 除了刚才说的,不要以任何其他方式重新分配关键字;
     * 下面是一个分裂后并重新分配的样子;
     *
     *   +----+----+----+----+  +----+----+----+----+
     *   |  1 |  2 |    |    |->|  3 |  4 |  5 |    |
     *   +----+----+----+----+  +----+----+----+----+
     *   leaf0                  leaf3
     *
     * When a leaf splits, it returns the first entry in the right node as the
     * split key. In this example, 3 is the split key. After leaf0 splits, inner
     * inserts the new key and child pointer into itself and hits case 0 (i.e. it
     * does not overflow). The tree looks like this:
     *
     * 当一个叶子分裂时，它返回右兄弟节点中的第一个条目作为split_key;
     * 刚才的例子,关键字3就是split_key;
     * split_key是用来索引新的叶子的
     * 3提升到父结点中,犹豫父结点还有富余空间,它不会触发溢出逻辑;
     *
     *                          inner
     *                          +--+--+--+--+
     *                          | 3|10|20|  |
     *                          +--+--+--+--+
     *                         /   |  |   \
     *                 _______/    |  |    \_________
     *                /            |   \             \
     *   +--+--+--+--+  +--+--+--+--+  +--+--+--+--+  +--+--+--+--+
     *   | 1| 2|  |  |->| 3| 4| 5|  |->|11|12|13|  |->|21|22|23|  |
     *   +--+--+--+--+  +--+--+--+--+  +--+--+--+--+  +--+--+--+--+
     *   leaf0          leaf3          leaf1          leaf2
     *
     * When an inner node splits, the first d entries are kept in the left node
     * and the last d entries are moved to the right node. The middle entry is
     * moved (not copied) up as the split key. For example, we would split the
     * following order 2 inner node
     *
     * 当内部结点分裂时，第一个d关键字保留在左边结点，最后d项移动到右边节点。
     * 中间的关键字作为split_key被移动或者叫被提升(而不是复制)
     * 例如，我们将拆分以下order为2的内部节点
     *
     *
     *   +---+---+---+---+
     *   | 1 | 2 | 3 | 4 | 5
     *   +---+---+---+---+
     *
     * into the following two inner nodes
     * 分裂进入以下两个内部节点
     *   +---+---+---+---+  +---+---+---+---+
     *   | 1 | 2 |   |   |  | 4 | 5 |   |   |
     *   +---+---+---+---+  +---+---+---+---+
     *
     * with a split key of 3.
     * 将⌈M/2⌉的关键字上移至其双亲结点
     * cell(元素数量/2) 也就是 ceil(5/2) 第3个元素,就是也3这个关键字
     *
     * DO NOT redistribute entries in any other way besides what we have
     * described. For example, do not move entries between nodes to avoid
     * splitting.
     *
     * 除了我们所描述的，不要以任何其他方式重新分配项。例如，不要在节点之间移动条目，以避免分裂。
     *
     * Our B+ trees do not support duplicate entries with the same key. If a
     * duplicate key is inserted into a leaf node, the tree is left unchanged
     * and a BPlusTreeException is raised.
     *
     * 我们的B+树不支持具有相同键的重复条目。
     * 如果在叶子节点中插入了重复的键，则树将保持不变，并引发BPlusTreeException。
     */
    public abstract Optional<Pair<DataBox, Long>> put(DataBox key, RecordId rid);

    /**
     * n.bulkLoad(data, fillFactor) bulk loads pairs of (k, r) from data into
     * the tree with the given fill factor.
     * bulk load 将数据对批量加载到树中,加载的时候需要设置一个填充因子
     *
     * This method is very similar to n.put, with a couple of differences:
     * 这个方法与put非常相似，但有几个不同之处:
     *
     * 1. Leaf nodes do not fill up to 2*d+1 and split, but rather, fill up to
     * be 1 record more than fillFactor full, then "splits" by creating a right
     * sibling that contains just one record (leaving the original node with
     * the desired fill factor).
     * 1. 叶节点在put时不会到2*d+1阈值时进行分裂，而是put的数量比fillFactor填充因子多1条记录，
     * 然后通过创建只包含一条记录的右兄弟节点进行"分裂"(让原始节点保留所需的填充因子)。
     *
     * 2. Inner nodes should repeatedly try to bulk load the rightmost child
     * until either the inner node is full (in which case it should split)
     * or there is no more data.
     * 2. 内部节点应该反复尝试批量加载最右边的子节点，
     * 直到内部节点满了(在这种情况下，它应该分裂)或者没有更多的数据。
     *
     * fillFactor should ONLY be used for determining how full leaf nodes are
     * (not inner nodes), and calculations should round up, i.e. with d=5
     * and fillFactor=0.75, leaf nodes should be 8/10 full.
     * 填充因子应该只用于确定完整的叶节点(而不是内部节点)，计算应该四舍五入，
     * 即d=5和填充因子=0.75时,叶节点的数据达到8/10视为满了。
     *
     * You can assume that 0 < fillFactor <= 1 for testing purposes, and that
     * a fill factor outside of that range will result in undefined behavior
     * (you're free to handle those cases however you like).
     * 为了测试的目的，您可以假设0 < fillFactor <= 1，
     * 并且填充因子超出该范围将导致未定义的行为(您可以自由地处理您喜欢的情况)
     */
    public abstract Optional<Pair<DataBox, Long>> bulkLoad(Iterator<Pair<DataBox, RecordId>> data,
            float fillFactor);

    /**
     * n.remove(k) removes the key k and its corresponding record id from the
     * subtree rooted by n, or does nothing if the key k is not in the subtree.
     * REMOVE SHOULD NOT REBALANCE THE TREE. Simply delete the key and
     * corresponding record id. For example, running inner.remove(2) on the
     * example tree above would produce the following tree.
     *
     * 移除键k和它对应的记录id
     * 以n为根的子树，如果键k不在子树中，则不执行任何操作。
     * 移除不应该重新平衡树。只需删除关键字和对应的记录id。
     * 例如，在上面的示例中,我们删除一个2后将生成下面的树。
     *
     *                               inner
     *                               +----+----+----+----+
     *                               | 10 | 20 |    |    |
     *                               +----+----+----+----+
     *                              /     |     \
     *                         ____/      |      \____
     *                        /           |           \
     *   +----+----+----+----+  +----+----+----+----+  +----+----+----+----+
     *   |  1 |  3 |    |    |->| 11 | 12 | 13 |    |->| 21 | 22 | 23 |    |
     *   +----+----+----+----+  +----+----+----+----+  +----+----+----+----+
     *   leaf0                  leaf1                  leaf2
     *
     * Running inner.remove(1) on this tree would produce the following tree:
     *
     * 继续再删除一个1
     *
     *                               inner
     *                               +----+----+----+----+
     *                               | 10 | 20 |    |    |
     *                               +----+----+----+----+
     *                              /     |     \
     *                         ____/      |      \____
     *                        /           |           \
     *   +----+----+----+----+  +----+----+----+----+  +----+----+----+----+
     *   |  3 |    |    |    |->| 11 | 12 | 13 |    |->| 21 | 22 | 23 |    |
     *   +----+----+----+----+  +----+----+----+----+  +----+----+----+----+
     *   leaf0                  leaf1                  leaf2
     *
     * Running inner.remove(3) would then produce the following tree:
     *
     * 接着再删除一个3
     *
     *                               inner
     *                               +----+----+----+----+
     *                               | 10 | 20 |    |    |
     *                               +----+----+----+----+
     *                              /     |     \
     *                         ____/      |      \____
     *                        /           |           \
     *   +----+----+----+----+  +----+----+----+----+  +----+----+----+----+
     *   |    |    |    |    |->| 11 | 12 | 13 |    |->| 21 | 22 | 23 |    |
     *   +----+----+----+----+  +----+----+----+----+  +----+----+----+----+
     *   leaf0                  leaf1                  leaf2
     *
     * Again, do NOT rebalance the tree.
     *
     * 最后,因为课程的要求,我们把某个结点删空了,可以暂时不需要重新平衡树;
     */
    public abstract void remove(DataBox key);

    // Helpers /////////////////////////////////////////////////////////////////
    /** Get the page on which this node is persisted. */
    // 获取此结点持久化的页。
    abstract Page getPage();

    // Pretty Printing /////////////////////////////////////////////////////////
    /**
     * S-expressions (or sexps) are a compact way of encoding nested tree-like
     * structures (sort of like how JSON is a way of encoding nested dictionaries
     * and lists). n.toSexp() returns an sexp encoding of the subtree rooted by
     * n. For example, the following tree:
     *
     *                      +---+
     *                      | 3 |
     *                      +---+
     *                     /     \
     *   +---------+---------+  +---------+---------+
     *   | 1:(1 1) | 2:(2 2) |  | 3:(3 3) | 4:(4 4) |
     *   +---------+---------+  +---------+---------+
     *
     * has the following sexp
     *
     *   (((1 (1 1)) (2 (2 2))) 3 ((3 (3 3)) (4 (4 4))))
     *
     * Here, (1 (1 1)) represents the mapping from key 1 to record id (1, 1).
     */
    // 在调试的方式方将树以进行可阅读性的字符输出
    public abstract String toSexp();

    /**
     * n.toDot() returns a fragment of a DOT file that draws the subtree rooted
     * at n.
     *
     * 返回结点的一个片段,主要用来绘图
     */
    public abstract String toDot();

    // Serialization ///////////////////////////////////////////////////////////
    /** n.toBytes() serializes n. */
    // 序列化方法,将结点序列化为字节数组
    public abstract byte[] toBytes();

    /**
     * BPlusNode.fromBytes(m, p) loads a BPlusNode from page `pageNum`.
     */
    // 根据元数据,缓存管理器,上下文锁对象和页码这些信息,把结点反序列化回BPlusNode
    public static BPlusNode fromBytes(BPlusTreeMetadata metadata, BufferManager bufferManager,
                                      LockContext treeContext, long pageNum) {
        Page p = bufferManager.fetchPage(treeContext, pageNum);
        try {
            Buffer buf = p.getBuffer();
            byte b = buf.get();
            if (b == 1) {
                return LeafNode.fromBytes(metadata, bufferManager, treeContext, pageNum);
            } else if (b == 0) {
                return InnerNode.fromBytes(metadata, bufferManager, treeContext, pageNum);
            } else {
                String msg = String.format("Unexpected byte %b.", b);
                throw new IllegalArgumentException(msg);
            }
        } finally {
            p.unpin();
        }
    }
}
