package edu.berkeley.cs186.database.query.join;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.query.QueryOperator;

/**
 * Performs an equijoin between two relations on leftColumnName and
 * rightColumnName respectively using the Page Nested Loop Join algorithm.
 *
 * 在leftColumnName和上的两个关系之间执行等价连接
 * rightColumnName分别使用页面嵌套循环连接算法。
 */
public class PNLJOperator extends BNLJOperator {
    public PNLJOperator(QueryOperator leftSource,
                 QueryOperator rightSource,
                 String leftColumnName,
                 String rightColumnName,
                 TransactionContext transaction) {
        super(leftSource,
              rightSource,
              leftColumnName,
              rightColumnName,
              transaction);

        joinType = JoinType.PNLJ;
        numBuffers = 3;
    }
}
