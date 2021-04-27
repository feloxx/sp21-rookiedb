package edu.berkeley.cs186.database.query.join;

import java.util.*;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.query.JoinOperator;
import edu.berkeley.cs186.database.query.QueryOperator;
import edu.berkeley.cs186.database.table.Record;

/**
 * Performs an equijoin between two relations on leftColumnName and
 * rightColumnName respectively using the Block Nested Loop Join algorithm.
 *
 * 在leftColumnName和上的两个关系之间执行等价连接
 * rightColumnName分别使用块嵌套循环连接算法。
 *
 * 左block 右page
 * block 是在缓存中
 */
public class BNLJOperator extends JoinOperator {
    protected int numBuffers; // 页数

    // 构造方法
    // 主要对父类的一些成员赋值
    // 本身主要是得到一个该优化算法的估计值,用于后面的评估
    // 因为BNLJ用到了缓存,所以这里还需要对缓存进行一些操作
    public BNLJOperator(QueryOperator leftSource,
                        QueryOperator rightSource,
                        String leftColumnName,
                        String rightColumnName,
                        TransactionContext transaction) {
        super(leftSource, materialize(rightSource, transaction),
                leftColumnName, rightColumnName, transaction, JoinType.BNLJ
        );
        this.numBuffers = transaction.getWorkMemSize(); // 获得该事务分配的内存量
        this.stats = this.estimateStats();
    }

    @Override
    public Iterator<Record> iterator() {
        return new BNLJIterator();
    }

    // IO成本估计
    @Override
    public int estimateIOCost() {
        //This method implements the IO cost estimation of the Block Nested Loop Join
        int usableBuffers = numBuffers - 2;
        int numLeftPages = getLeftSource().estimateStats().getNumPages();
        int numRightPages = getRightSource().estimateIOCost();
        return ((int) Math.ceil((double) numLeftPages / (double) usableBuffers)) * numRightPages +
               getLeftSource().estimateIOCost();
    }

    /**
     * A record iterator that executes the logic for a simple nested loop join.
     * Look over the implementation in SNLJOperator if you want to get a feel
     * for the fetchNextRecord() logic.
     */
    private class BNLJIterator implements Iterator<Record>{
        // Iterator over all the records of the left source 左表的常规迭代器
        private Iterator<Record> leftSourceIterator;
        // Iterator over all the records of the right source 右表回溯迭代器
        private BacktrackingIterator<Record> rightSourceIterator;
        // Iterator over records in the current block of left pages 左块回溯迭代器
        private BacktrackingIterator<Record> leftBlockIterator;
        // Iterator over records in the current right page 右页回溯迭代器
        private BacktrackingIterator<Record> rightPageIterator;
        // The current record from the left relation 左关系中的当前记录
        private Record leftRecord;
        // The next record to return
        private Record nextRecord;

        private BNLJIterator() {
            super();
            this.leftSourceIterator = getLeftSource().iterator();
            this.fetchNextLeftBlock();

            this.rightSourceIterator = getRightSource().backtrackingIterator();
            this.rightSourceIterator.markNext();
            this.fetchNextRightPage();

            this.nextRecord = null;
        }

        /**
         * Fetch the next block of records from the left source.
         * leftBlockIterator should be set to a backtracking iterator over up to
         * B-2 pages of records from the left source, and leftRecord should be
         * set to the first record in this block.
         *
         * If there are no more records in the left source, this method should
         * do nothing.
         *
         * You may find QueryOperator#getBlockIterator useful here.
         *
         * 从左源获取下一个记录块。
         * leftBlockIterator应该被设置为一个回溯迭代器，从左源到B-2页的记录，并且leftRecord应该被设置为这个块中的第一个记录。
         * 如果左侧源中没有更多的记录，则此方法不应执行任何操作。
         * 你可以在这里找到QueryOperator#getBlockIterator。
         *
         * 暂时理解是将右表按照条件拆分,这里的条件就是numBuffers - 2页,拆分后的数据可以理解为多页,既前面说的块
         * 可以理解将数据拆分放到块迭代器中
         */
        private void fetchNextLeftBlock() {
            // TODO(proj3_part1): implement

            if (this.leftSourceIterator.hasNext()) {
                this.leftBlockIterator = QueryOperator.getBlockIterator(leftSourceIterator, getLeftSource().getSchema(), numBuffers - 2);
                this.leftBlockIterator.markNext();
                this.leftRecord = leftBlockIterator.next();
            }

            // leftBlockIterator = QueryOperator.getBlockIterator(leftSourceIterator, getLeftSource().getSchema(), numBuffers - 2);
            // if (!leftBlockIterator.hasNext()){
            //     leftBlockIterator = null;
            //     leftRecord = null;
            //     rightPageIterator = null;
            //     rightSourceIterator = null;
            //     throw new NoSuchElementException("All Done!");
            // }
            // // Mark the first record of the block
            // leftBlockIterator.markNext();
            // leftRecord = leftBlockIterator.next();
        }

        /**
         * Fetch the next page of records from the right source.
         * rightPageIterator should be set to a backtracking iterator over up to
         * one page of records from the right source.
         *
         * If there are no more records in the right source, this method should
         * do nothing.
         *
         * You may find QueryOperator#getBlockIterator useful here.
         *
         * 从源获取下一页记录。
         * rightPageIterator应该被设置为一个回溯迭代器，最多可以回溯来自右源的一页记录。
         *
         * 暂时理解是将右表按照条件拆分,这里的条件就是1页
         */
        private void fetchNextRightPage() {
            // TODO(proj3_part1): implement

            if (this.rightSourceIterator.hasNext()) {
                this.rightPageIterator = QueryOperator.getBlockIterator(rightSourceIterator, getRightSource().getSchema(), 1);
                this.rightPageIterator.markNext();
            }
        }

        /**
         * Returns the next record that should be yielded from this join,
         * or null if there are no more records to join.
         *
         * You may find JoinOperator#compare useful here. (You can call compare
         * function directly from this file, since BNLJOperator is a subclass
         * of JoinOperator).
         *
         * 返回应从该连接中生成的下一个记录，如果没有更多要连接的记录，则返回null。
         * 你可以在这里找到JoinOperator#compare。(你可以直接从这个文件调用compare函数，因为BNLJOperator是JoinOperator的一个子类)。
         *
         * 课件中的伪代码
         * for each block of B−2 pages Br in R:  遍历左表 R 到 Br 缓存中
         * 	for each page ps in S:               遍历右表 S 到 ps 一页中
         * 		for each record ri in Br:          遍历 Br缓存
         * 			for each record sj in ps:        遍历 ps页
         * 				if θ(ri,sj): join逻辑匹配
         * 					yield <ri, sj>
         *
         * 作业中提到的要考虑的四种情况
         * 情况1: 右页的迭代器有一个要生产的值
         * 情况2: 右页迭代器没有要产生的值，但是左边的块迭代器有
         * 情况3: 右页和左块迭代器都不需要产生值，但是右页数据量更大
         * 情况4: 右页和左块迭代器都没有值，也没有更多的右页，但是仍然有左块
         */
        private Record fetchNextRecord() {
            // TODO(proj3_part1): implement

            if (this.leftRecord == null) {
                return null;
            }
            while (true) {
                if (this.rightPageIterator.hasNext()) {
                    // 右页前进,最内循环的匹配成功逻辑
                    Record rightRecord = rightPageIterator.next();
                    if (compare(this.leftRecord, rightRecord) == 0) {
                        return this.leftRecord.concat(rightRecord);
                    }
                } else if (this.leftBlockIterator.hasNext()) {
                    // 左块前进
                    this.leftRecord = this.leftBlockIterator.next();
                    this.rightPageIterator.reset();
                    this.rightPageIterator.markNext();
                } else if (this.rightSourceIterator.hasNext()) {
                    // 右源前进
                    fetchNextRightPage();
                    this.leftBlockIterator.reset();
                    this.leftBlockIterator.markNext();
                    this.leftRecord = this.leftBlockIterator.next();
                } else if (this.leftSourceIterator.hasNext()) {
                    // 左源前进
                    fetchNextLeftBlock();
                    this.rightSourceIterator.reset();
                    this.rightSourceIterator.markNext();
                    fetchNextRightPage();
                } else {
                    return null;
                }
            }
        }

        /**
         * @return true if this iterator has another record to yield, otherwise
         * false
         */
        @Override
        public boolean hasNext() {
            if (this.nextRecord == null) this.nextRecord = fetchNextRecord();
            return this.nextRecord != null;
        }

        /**
         * @return the next record from this iterator
         * @throws NoSuchElementException if there are no more records to yield
         */
        @Override
        public Record next() {
            if (!this.hasNext()) throw new NoSuchElementException();
            Record nextRecord = this.nextRecord;
            this.nextRecord = null;
            return nextRecord;
        }
    }
}
