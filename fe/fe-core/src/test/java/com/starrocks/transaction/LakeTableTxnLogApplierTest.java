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

package com.starrocks.transaction;

import com.starrocks.lake.LakeTable;
import com.starrocks.lake.compaction.CompactionTxnCommitAttachment;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class LakeTableTxnLogApplierTest extends LakeTableTestHelper {
    @Test
    public void testCommitAndApply() {
        LakeTable table = buildLakeTable();
        LakeTableTxnLogApplier applier = new LakeTableTxnLogApplier(table);
        TransactionState state = newTransactionState();
        state.setTransactionStatus(TransactionStatus.COMMITTED);
        PartitionCommitInfo partitionCommitInfo = new PartitionCommitInfo(physicalPartitionId, 2, 0);
        TableCommitInfo tableCommitInfo = new TableCommitInfo(tableId);
        tableCommitInfo.addPartitionCommitInfo(partitionCommitInfo);

        applier.applyCommitLog(state, tableCommitInfo);
        Assertions.assertEquals(1, table.getPartition(partitionId).getDefaultPhysicalPartition().getVisibleVersion());
        Assertions.assertEquals(3, table.getPartition(partitionId).getDefaultPhysicalPartition().getNextVersion());

        state.setTransactionStatus(TransactionStatus.VISIBLE);
        partitionCommitInfo.setVersionTime(System.currentTimeMillis());
        applier.applyVisibleLog(state, tableCommitInfo, /*unused*/null);
        Assertions.assertEquals(2, table.getPartition(partitionId).getDefaultPhysicalPartition().getVisibleVersion());
        Assertions.assertEquals(3, table.getPartition(partitionId).getDefaultPhysicalPartition().getNextVersion());
        Assertions.assertEquals(partitionCommitInfo.getVersionTime(),
                table.getPartition(partitionId).getDefaultPhysicalPartition()
                        .getVisibleVersionTime());
    }

    @Test
    public void testCommitAndApplyCompaction() {
        LakeTable table = buildLakeTable();
        LakeTableTxnLogApplier applier = new LakeTableTxnLogApplier(table);
        TransactionState state = newCompactionTransactionState();
        CompactionTxnCommitAttachment attachment = new CompactionTxnCommitAttachment(true);
        state.setTxnCommitAttachment(attachment);
        state.setTransactionStatus(TransactionStatus.COMMITTED);
        PartitionCommitInfo partitionCommitInfo = new PartitionCommitInfo(physicalPartitionId, 2, 0);
        TableCommitInfo tableCommitInfo = new TableCommitInfo(tableId);
        tableCommitInfo.addPartitionCommitInfo(partitionCommitInfo);

        applier.applyCommitLog(state, tableCommitInfo);
        Assertions.assertEquals(1, table.getPartition(partitionId).getDefaultPhysicalPartition().getVisibleVersion());
        Assertions.assertEquals(3, table.getPartition(partitionId).getDefaultPhysicalPartition().getNextVersion());

        state.setTransactionStatus(TransactionStatus.VISIBLE);
        partitionCommitInfo.setVersionTime(System.currentTimeMillis());
        applier.applyVisibleLog(state, tableCommitInfo, /*unused*/null);
        Assertions.assertEquals(2, table.getPartition(partitionId).getDefaultPhysicalPartition().getVisibleVersion());
        Assertions.assertEquals(3, table.getPartition(partitionId).getDefaultPhysicalPartition().getNextVersion());
        Assertions.assertEquals(partitionCommitInfo.getVersionTime(),
                table.getPartition(partitionId).getDefaultPhysicalPartition()
                        .getVisibleVersionTime());
    }

    @Test
    public void testApplyCommitLogWithDroppedPartition() {
        LakeTable table = buildLakeTable();
        LakeTableTxnLogApplier applier = new LakeTableTxnLogApplier(table);
        TransactionState state = newTransactionState();
        TableCommitInfo tableCommitInfo = new TableCommitInfo(tableId);
        PartitionCommitInfo partitionCommitInfo = new PartitionCommitInfo(partitionId - 1, 2, 0);
        tableCommitInfo.addPartitionCommitInfo(partitionCommitInfo);

        applier.applyCommitLog(state, tableCommitInfo);
        Assertions.assertEquals(1, table.getPartition(partitionId).getDefaultPhysicalPartition().getVisibleVersion());
        Assertions.assertEquals(2, table.getPartition(partitionId).getDefaultPhysicalPartition().getNextVersion());

        state.setTransactionStatus(TransactionStatus.VISIBLE);
        partitionCommitInfo.setVersionTime(System.currentTimeMillis());
        applier.applyVisibleLog(state, tableCommitInfo, /*unused*/null);
        Assertions.assertEquals(1, table.getPartition(partitionId).getDefaultPhysicalPartition().getVisibleVersion());
        Assertions.assertEquals(2, table.getPartition(partitionId).getDefaultPhysicalPartition().getNextVersion());
    }
}
