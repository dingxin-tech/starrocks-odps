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

package com.starrocks.sql.optimizer.operator.scalar;

import com.google.common.base.Preconditions;
import com.starrocks.analysis.BinaryType;
import com.starrocks.sql.optimizer.operator.OperatorType;

import java.util.List;
import java.util.Objects;

public class BinaryPredicateOperator extends PredicateOperator {
    private BinaryType type;

    public BinaryPredicateOperator(BinaryType type, ScalarOperator... arguments) {
        super(OperatorType.BINARY, arguments);
        this.type = type;
        Preconditions.checkState(arguments.length == 2);
        incrDepth(arguments);
    }

    public BinaryPredicateOperator(BinaryType type, List<ScalarOperator> arguments) {
        super(OperatorType.BINARY, arguments);
        this.type = type;
        Preconditions.checkState(arguments.size() == 2);
        incrDepth(arguments);
    }

    public void setBinaryType(BinaryType type) {
        this.type = type;
    }

    public BinaryType getBinaryType() {
        return type;
    }

    public void swap() {
        ScalarOperator c0 = getChild(0);
        ScalarOperator c1 = getChild(1);
        setChild(0, c1);
        setChild(1, c0);
    }

    @Override
    public <R, C> R accept(ScalarOperatorVisitor<R, C> visitor, C context) {
        return visitor.visitBinaryPredicate(this, context);
    }

    public BinaryPredicateOperator commutative() {
        return new BinaryPredicateOperator(this.getBinaryType().commutative(),
                this.getChild(1),
                this.getChild(0));
    }

    public BinaryPredicateOperator negative() {
        if (this.getBinaryType().hasNegative()) {
            return new BinaryPredicateOperator(this.getBinaryType().negative(), this.getChildren());
        } else {
            return null;
        }
    }

    /**
     * For Non-Strict Monotonic function, we need to convert
     * 1. > to >=, and < to <=
     * 2. !=, not supported
     */
    public BinaryPredicateOperator normalizeNonStrictMonotonic() {
        if (getBinaryType() == BinaryType.NE) {
            return null;
        }
        BinaryPredicateOperator result = (BinaryPredicateOperator) clone();
        if (getBinaryType() == BinaryType.LT) {
            result.setBinaryType(BinaryType.LE);
        }
        if (getBinaryType() == BinaryType.GT) {
            result.setBinaryType(BinaryType.GE);
        }
        return result;
    }

    @Override
    public String toString() {
        return getChild(0).toString() + " " + type.toString() + " " + getChild(1).toString();
    }

    @Override
    public String debugString() {
        return getChild(0).debugString() + " " + type.toString() + " " + getChild(1).debugString();
    }

    @Override
    public boolean equalsSelf(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equalsSelf(o)) {
            return false;
        }
        BinaryPredicateOperator that = (BinaryPredicateOperator) o;
        return type == that.type;
    }

    @Override
    public int hashCodeSelf() {
        return Objects.hash(super.hashCodeSelf(), type);
    }

    @Override
    public boolean isNullable() {
        return !this.type.equals(BinaryType.EQ_FOR_NULL) && super.isNullable();
    }

    public static BinaryPredicateOperator eq(ScalarOperator lhs, ScalarOperator rhs) {
        return new BinaryPredicateOperator(BinaryType.EQ, lhs, rhs);
    }

    public static BinaryPredicateOperator ge(ScalarOperator lhs, ScalarOperator rhs) {
        return new BinaryPredicateOperator(BinaryType.GE, lhs, rhs);
    }

    public static BinaryPredicateOperator gt(ScalarOperator lhs, ScalarOperator rhs) {
        return new BinaryPredicateOperator(BinaryType.GT, lhs, rhs);
    }

    public static BinaryPredicateOperator ne(ScalarOperator lhs, ScalarOperator rhs) {
        return new BinaryPredicateOperator(BinaryType.NE, lhs, rhs);
    }

    public static BinaryPredicateOperator le(ScalarOperator lhs, ScalarOperator rhs) {
        return new BinaryPredicateOperator(BinaryType.LE, lhs, rhs);
    }

    public static BinaryPredicateOperator lt(ScalarOperator lhs, ScalarOperator rhs) {
        return new BinaryPredicateOperator(BinaryType.LT, lhs, rhs);
    }
}
