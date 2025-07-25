// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.google.common.collect.Lists;
import com.starrocks.analysis.BinaryType;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class PredicateSplitTest {
    @Test
    public void testSplitPredicate() {
        ScalarOperator predicate = null;
        PredicateSplit split = PredicateSplit.splitPredicate(predicate);
        Assertions.assertNotNull(split);
        Assertions.assertNull(split.getEqualPredicates());
        Assertions.assertNull(split.getRangePredicates());
        Assertions.assertNull(split.getResidualPredicates());

        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        ColumnRefOperator columnRef1 = columnRefFactory.create("col1", Type.INT, false);
        ColumnRefOperator columnRef2 = columnRefFactory.create("col2", Type.INT, false);
        BinaryPredicateOperator binaryPredicate = new BinaryPredicateOperator(
                BinaryType.EQ, columnRef1, columnRef2);
        BinaryPredicateOperator binaryPredicate2 = new BinaryPredicateOperator(
                BinaryType.GE, columnRef1, ConstantOperator.createInt(1));

        List<ScalarOperator> arguments = Lists.newArrayList();
        arguments.add(columnRef1);
        arguments.add(columnRef2);
        CallOperator callOperator = new CallOperator(FunctionSet.SUM, Type.INT, arguments);
        BinaryPredicateOperator binaryPredicate3 = new BinaryPredicateOperator(
                BinaryType.GE, callOperator, ConstantOperator.createInt(1));
        ScalarOperator andPredicate = Utils.compoundAnd(binaryPredicate, binaryPredicate2, binaryPredicate3);
        PredicateSplit result = PredicateSplit.splitPredicate(andPredicate);
        Assertions.assertEquals(binaryPredicate, result.getEqualPredicates());
        Assertions.assertEquals(binaryPredicate2, result.getRangePredicates());
        Assertions.assertEquals(binaryPredicate3, result.getResidualPredicates());
    }

    @Test
    public void testSplitPredicateWithMultiRange() {
        ColumnRefOperator a = new ColumnRefOperator(0, Type.INT, "a", false);
        ColumnRefOperator b = new ColumnRefOperator(1, Type.INT, "b", false);
        ColumnRefOperator c = new ColumnRefOperator(2, Type.INT, "c", false);
        ColumnRefOperator d = new ColumnRefOperator(3, Type.INT, "d", false);

        ScalarOperator rangePredicate = CompoundPredicateOperator.or(
                CompoundPredicateOperator.and(
                    BinaryPredicateOperator.ge(a, ConstantOperator.createInt(0)),
                    BinaryPredicateOperator.lt(a, ConstantOperator.createInt(3))
                ),
                CompoundPredicateOperator.and(
                    BinaryPredicateOperator.ge(a, ConstantOperator.createInt(4)),
                    BinaryPredicateOperator.lt(a, ConstantOperator.createInt(7))
                )
        );
        BinaryPredicateOperator equalPredicate = BinaryPredicateOperator.eq(c, d);
        InPredicateOperator inPredicate = new InPredicateOperator(
                b,
                ConstantOperator.createInt(0), ConstantOperator.createInt(1));
        ScalarOperator otherPredicate = CompoundPredicateOperator.and(
                BinaryPredicateOperator.ne(b, ConstantOperator.createInt(1)),
                inPredicate
        );

        {
            {
                ScalarOperator predicate = CompoundPredicateOperator.not(
                        CompoundPredicateOperator.or(
                                CompoundPredicateOperator.and(
                                        BinaryPredicateOperator.eq(a, ConstantOperator.createInt(0)),
                                        BinaryPredicateOperator.eq(b, ConstantOperator.createInt(3))
                                ),
                                CompoundPredicateOperator.and(
                                        BinaryPredicateOperator.eq(a, ConstantOperator.createInt(4)),
                                        BinaryPredicateOperator.eq(b, ConstantOperator.createInt(7))
                                )
                        ));
                PredicateSplit predicateSplit = PredicateSplit.splitPredicate(predicate);
                Assertions.assertEquals("0: a != 0 OR 1: b != 3 AND 0: a != 4 OR 1: b != 7",
                        predicateSplit.getRangePredicates().toString());
            }
        }

        {
            {
                ScalarOperator predicate = CompoundPredicateOperator.not(
                        CompoundPredicateOperator.and(
                                CompoundPredicateOperator.or(
                                        BinaryPredicateOperator.eq(a, ConstantOperator.createInt(0)),
                                        BinaryPredicateOperator.eq(b, ConstantOperator.createInt(3))
                                ),
                                CompoundPredicateOperator.or(
                                        BinaryPredicateOperator.eq(a, ConstantOperator.createInt(4)),
                                        BinaryPredicateOperator.eq(b, ConstantOperator.createInt(7))
                                )
                        ));
                PredicateSplit predicateSplit = PredicateSplit.splitPredicate(predicate);
                Assertions.assertEquals("0: a != 0 AND 1: b != 3 OR 0: a != 4 AND 1: b != 7",
                        predicateSplit.getRangePredicates().toString());
            }
        }

        {
            ScalarOperator predicate = CompoundPredicateOperator.and(rangePredicate, equalPredicate, otherPredicate);

            PredicateSplit predicateSplit = PredicateSplit.splitPredicate(predicate);
            Assertions.assertEquals(equalPredicate, predicateSplit.getEqualPredicates());
            Assertions.assertEquals("0: a >= 0 AND 0: a < 3 OR 0: a >= 4 AND 0: a < 7 AND 1: b != 1",
                    predicateSplit.getRangePredicates().toString());
            Assertions.assertEquals(inPredicate, predicateSplit.getResidualPredicates());
        }
        {
            ScalarOperator residual = CompoundPredicateOperator.or(equalPredicate, otherPredicate);
            ScalarOperator predicate = CompoundPredicateOperator.and(residual, rangePredicate);
            PredicateSplit predicateSplit = PredicateSplit.splitPredicate(predicate);
            Assertions.assertNull(predicateSplit.getEqualPredicates());
            Assertions.assertEquals(rangePredicate, predicateSplit.getRangePredicates());
            Assertions.assertEquals(residual, predicateSplit.getResidualPredicates());
        }

        {
            ScalarOperator predicate = CompoundPredicateOperator.and(
                    CompoundPredicateOperator.or(
                        BinaryPredicateOperator.eq(a, ConstantOperator.createInt(0)),
                        BinaryPredicateOperator.eq(b, ConstantOperator.createInt(3))
                    ),
                    CompoundPredicateOperator.or(
                        BinaryPredicateOperator.eq(a, ConstantOperator.createInt(4)),
                        BinaryPredicateOperator.eq(b, ConstantOperator.createInt(7))
                    )
            );
            PredicateSplit predicateSplit = PredicateSplit.splitPredicate(predicate);
            Assertions.assertEquals(predicate, predicateSplit.getRangePredicates());
        }

        {
            IsNullPredicateOperator predicate = new IsNullPredicateOperator(a);
            PredicateSplit predicateSplit = PredicateSplit.splitPredicate(predicate);
            Assertions.assertEquals(predicate, predicateSplit.getResidualPredicates());
        }

        {
            ScalarOperator predicate = CompoundPredicateOperator.or(
                    BinaryPredicateOperator.ge(a, ConstantOperator.createInt(2000)),
                    new IsNullPredicateOperator(a)
            );
            PredicateSplit predicateSplit = PredicateSplit.splitPredicate(predicate);
            Assertions.assertEquals(predicate, predicateSplit.getResidualPredicates());
        }
    }

    @Test
    public void testSplitPredicate2() {
        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        ColumnRefOperator columnRef1 = columnRefFactory.create("col1", Type.INT, false);
        BinaryPredicateOperator predicate1 = new BinaryPredicateOperator(
                BinaryType.EQ, columnRef1, ConstantOperator.createInt(1));
        ScalarOperator predicate2 =  CompoundPredicateOperator.or(
                new BinaryPredicateOperator(BinaryType.EQ, columnRef1, ConstantOperator.createInt(1)),
                new BinaryPredicateOperator(BinaryType.EQ, columnRef1, ConstantOperator.createInt(2))
        );

        ScalarOperator andPredicate = Utils.compoundAnd(predicate1, predicate2);
        PredicateSplit result = PredicateSplit.splitPredicate(andPredicate);
        Assertions.assertNull(result.getEqualPredicates());
        Assertions.assertNull(result.getResidualPredicates());
        Assertions.assertEquals(predicate1, result.getRangePredicates());
    }

    @Test
    public void testSplitPredicate3() {
        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        ColumnRefOperator columnRef1 = columnRefFactory.create("col1", Type.INT, false);
        BinaryPredicateOperator predicate1 = new BinaryPredicateOperator(
                BinaryType.EQ, columnRef1, ConstantOperator.createInt(1));
        ScalarOperator predicate2 =  CompoundPredicateOperator.or(
                new BinaryPredicateOperator(BinaryType.EQ, columnRef1, ConstantOperator.createInt(3)),
                new BinaryPredicateOperator(BinaryType.EQ, columnRef1, ConstantOperator.createInt(2))
        );

        ScalarOperator andPredicate = Utils.compoundAnd(predicate1, predicate2);
        PredicateSplit result = PredicateSplit.splitPredicate(andPredicate);
        System.out.println(result);
        Assertions.assertNull(result.getEqualPredicates());
        Assertions.assertNull(result.getResidualPredicates());
        Assertions.assertEquals(ConstantOperator.FALSE, result.getRangePredicates());
    }
}
