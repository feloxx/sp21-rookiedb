package edu.berkeley.cs186.database.query.join;

import java.util.*;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.query.JoinOperator;
import edu.berkeley.cs186.database.query.QueryOperator;
import edu.berkeley.cs186.database.table.Record;

/**
 * Performs an equijoin between two relations on leftColumnName and
 * rightColumnName respectively using the Simple Nested Loop Join algorithm.
 *
 * 在leftColumnName和上的两个关系之间执行等价连接
 * rightColumnName分别使用简单嵌套循环连接算法。
 */
public class SNLJOperator extends JoinOperator {

    // 构造方法
    // 主要是对父类中的一些成员赋值
    // 本身主要是得到一个该优化算法的估计值,用于后面的评估
    public SNLJOperator(QueryOperator leftSource,
                        QueryOperator rightSource,
                        String leftColumnName,
                        String rightColumnName,
                        TransactionContext transaction) {
        super(leftSource, materialize(rightSource, transaction),
              leftColumnName, rightColumnName, transaction, JoinType.SNLJ);
        this.stats = this.estimateStats();
    }

    @Override
    public Iterator<Record> iterator() {
        return new SNLJIterator();
    }

    // IO成本估计
    @Override
    public int estimateIOCost() {
        int numLeftRecords = getLeftSource().estimateStats().getNumRecords();
        int numRightPages = getRightSource().estimateStats().getNumPages();
        return numLeftRecords * numRightPages + getLeftSource().estimateIOCost();
    }

    /**
     * A record iterator that executes the logic for a simple nested loop join.
     * Note that the left table is the "outer" loop and the right table is the
     * "inner" loop.
     *
     * 一个记录迭代器，用于执行简单嵌套循环联接的逻辑。
     * 注意，左边的表是"外部"循环，右边的表是"内部"循环。
     */
    private class SNLJIterator implements Iterator<Record> {
        // Iterator over all the records of the left relation 左表常规迭代器
        private Iterator<Record> leftSourceIterator;
        // Iterator over all the records of the right relation 右表回溯迭代器
        private BacktrackingIterator<Record> rightSourceIterator;
        // The current record from the left relation 当前记录的做关系
        private Record leftRecord;
        // The next record to return 要返回的下一个记录
        private Record nextRecord;

        // 构造方法从join父类中获取左右表的迭代器
        // 准备开始迭代
        public SNLJIterator() {
            super();
            this.leftSourceIterator = getLeftSource().iterator();
            if (leftSourceIterator.hasNext()) leftRecord = leftSourceIterator.next();

            this.rightSourceIterator = getRightSource().backtrackingIterator();
            this.rightSourceIterator.markNext();
        }

        /**
         * Returns the next record that should be yielded from this join,
         * or null if there are no more records to join.
         *
         * 返回应从该连接中生成的下一个记录，
         * 如果没有更多记录要连接，则为null。
         *
         * 对应伪代码
         * for each record ri in R:
         * 	for each record sj in S:
         * 		if θ(ri,sj):
         * 			yield <ri, sj>
         */
        private Record fetchNextRecord() {
            if (leftRecord == null) {
                // The left source was empty, nothing to fetch
                return null;
            }
            while(true) {
                if (this.rightSourceIterator.hasNext()) {
                    // there's a next right record, join it if there's a match
                    Record rightRecord = rightSourceIterator.next();
                    if (compare(leftRecord, rightRecord) == 0) {
                        return leftRecord.concat(rightRecord);
                    }
                } else if (leftSourceIterator.hasNext()){
                    // there's no more right records but there's still left
                    // records. Advance left and reset right
                    // 没有更多的右记录了,但还有左记录,继续迭代左并把右重置
                    this.leftRecord = leftSourceIterator.next();
                    this.rightSourceIterator.reset();
                } else {
                    // if you're here then there are no more records to fetch
                    return null;
                }
            }
        }

        @Override
        public boolean hasNext() {
            if (this.nextRecord == null) this.nextRecord = fetchNextRecord();
            return this.nextRecord != null;
        }

        @Override
        public Record next() {
            if (!this.hasNext()) throw new NoSuchElementException();
            Record nextRecord = this.nextRecord;
            this.nextRecord = null;
            return nextRecord;
        }
    }

}

