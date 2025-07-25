// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//

package com.starrocks.load.loadv2.dpp;

import com.starrocks.load.loadv2.etl.EtlJobConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class StarRocksRangePartitionerTest {

    @Test
    public void testRangePartitioner() {
        List<Object> startKeys = new ArrayList<>();
        startKeys.add(Integer.valueOf(0));
        List<Object> endKeys = new ArrayList<>();
        endKeys.add(Integer.valueOf(100));
        EtlJobConfig.EtlPartition partition1 = new EtlJobConfig.EtlPartition(
                10000, startKeys, endKeys, false, false, 3);

        List<Object> startKeys2 = new ArrayList<>();
        startKeys2.add(Integer.valueOf(100));
        List<Object> endKeys2 = new ArrayList<>();
        endKeys2.add(Integer.valueOf(200));
        EtlJobConfig.EtlPartition partition2 = new EtlJobConfig.EtlPartition(
                10001, startKeys2, endKeys2, false, false, 4);

        List<Object> startKeys3 = new ArrayList<>();
        startKeys3.add(Integer.valueOf(200));
        List<Object> endKeys3 = new ArrayList<>();
        endKeys3.add(Integer.valueOf(300));
        EtlJobConfig.EtlPartition partition3 = new EtlJobConfig.EtlPartition(
                10002, startKeys3, endKeys3, false, false, 5);

        List<EtlJobConfig.EtlPartition> partitions = new ArrayList<>();
        partitions.add(partition1);
        partitions.add(partition2);
        partitions.add(partition3);

        List<String> partitionColumns = new ArrayList<>();
        partitionColumns.add("id");
        List<String> bucketColumns = new ArrayList<>();
        bucketColumns.add("key");
        EtlJobConfig.EtlPartitionInfo partitionInfo = new EtlJobConfig.EtlPartitionInfo(
                "RANGE", partitionColumns, bucketColumns, partitions);
        List<StarRocksRangePartitioner.PartitionRangeKey> partitionRangeKeys = new ArrayList<>();
        for (EtlJobConfig.EtlPartition partition : partitions) {
            StarRocksRangePartitioner.PartitionRangeKey partitionRangeKey = new StarRocksRangePartitioner.PartitionRangeKey();
            partitionRangeKey.isMinPartition = partition.isMinPartition;
            partitionRangeKey.isMaxPartition = partition.isMaxPartition;
            partitionRangeKey.startKeys = new DppColumns(partition.startKeys);
            partitionRangeKey.endKeys = new DppColumns(partition.endKeys);
            partitionRangeKeys.add(partitionRangeKey);
        }
        List<Integer> partitionKeyIndexes = new ArrayList<>();
        partitionKeyIndexes.add(0);
        StarRocksRangePartitioner rangePartitioner =
                new StarRocksRangePartitioner(partitionInfo, partitionKeyIndexes, partitionRangeKeys);
        int num = rangePartitioner.numPartitions();
        Assertions.assertEquals(3, num);

        List<Object> fields1 = new ArrayList<>();
        fields1.add(-100);
        fields1.add("name");
        DppColumns record1 = new DppColumns(fields1);
        int id1 = rangePartitioner.getPartition(record1);
        Assertions.assertEquals(-1, id1);

        List<Object> fields2 = new ArrayList<>();
        fields2.add(10);
        fields2.add("name");
        DppColumns record2 = new DppColumns(fields2);
        int id2 = rangePartitioner.getPartition(record2);
        Assertions.assertEquals(0, id2);

        List<Object> fields3 = new ArrayList<>();
        fields3.add(110);
        fields3.add("name");
        DppColumns record3 = new DppColumns(fields3);
        int id3 = rangePartitioner.getPartition(record3);
        Assertions.assertEquals(1, id3);

        List<Object> fields4 = new ArrayList<>();
        fields4.add(210);
        fields4.add("name");
        DppColumns record4 = new DppColumns(fields4);
        int id4 = rangePartitioner.getPartition(record4);
        Assertions.assertEquals(2, id4);

        List<Object> fields5 = new ArrayList<>();
        fields5.add(310);
        fields5.add("name");
        DppColumns record5 = new DppColumns(fields5);
        int id5 = rangePartitioner.getPartition(record5);
        Assertions.assertEquals(-1, id5);

        List<Object> fields6 = new ArrayList<>();
        fields6.add(null);
        fields6.add("name");
        DppColumns record6 = new DppColumns(fields6);
        int id6 = rangePartitioner.getPartition(record6);
        Assertions.assertEquals(-1, id6);
    }

    @Test
    public void testMinPartitionWithNull() {
        List<Object> startKeys = new ArrayList<>();
        List<Object> endKeys = new ArrayList<>();
        endKeys.add(Integer.valueOf(100));
        EtlJobConfig.EtlPartition partition1 = new EtlJobConfig.EtlPartition(
                10000, startKeys, endKeys, true, false, 3);

        List<Object> startKeys2 = new ArrayList<>();
        startKeys2.add(Integer.valueOf(100));
        List<Object> endKeys2 = new ArrayList<>();
        endKeys2.add(Integer.valueOf(200));
        EtlJobConfig.EtlPartition partition2 = new EtlJobConfig.EtlPartition(
                10001, startKeys2, endKeys2, false, false, 4);

        List<EtlJobConfig.EtlPartition> partitions = new ArrayList<>();
        partitions.add(partition1);
        partitions.add(partition2);

        List<String> partitionColumns = new ArrayList<>();
        partitionColumns.add("id");
        List<String> bucketColumns = new ArrayList<>();
        bucketColumns.add("key");
        EtlJobConfig.EtlPartitionInfo partitionInfo = new EtlJobConfig.EtlPartitionInfo(
                "RANGE", partitionColumns, bucketColumns, partitions);
        List<StarRocksRangePartitioner.PartitionRangeKey> partitionRangeKeys = new ArrayList<>();
        for (EtlJobConfig.EtlPartition partition : partitions) {
            StarRocksRangePartitioner.PartitionRangeKey partitionRangeKey = new StarRocksRangePartitioner.PartitionRangeKey();
            partitionRangeKey.isMinPartition = partition.isMinPartition;
            partitionRangeKey.isMaxPartition = partition.isMaxPartition;
            partitionRangeKey.startKeys = new DppColumns(partition.startKeys);
            partitionRangeKey.endKeys = new DppColumns(partition.endKeys);
            partitionRangeKeys.add(partitionRangeKey);
        }
        List<Integer> partitionKeyIndexes = new ArrayList<>();
        partitionKeyIndexes.add(0);
        StarRocksRangePartitioner rangePartitioner =
                new StarRocksRangePartitioner(partitionInfo, partitionKeyIndexes, partitionRangeKeys);
        int num = rangePartitioner.numPartitions();
        Assertions.assertEquals(2, num);

        List<Object> fields1 = new ArrayList<>();
        fields1.add(null);
        fields1.add("name");
        DppColumns record1 = new DppColumns(fields1);
        int id1 = rangePartitioner.getPartition(record1);
        Assertions.assertEquals(0, id1);
    }

    @Test
    public void testUnpartitionedPartitioner() {
        List<String> partitionColumns = new ArrayList<>();
        List<String> bucketColumns = new ArrayList<>();
        bucketColumns.add("key");
        EtlJobConfig.EtlPartitionInfo partitionInfo = new EtlJobConfig.EtlPartitionInfo(
                "UNPARTITIONED", null, bucketColumns, null);
        List<StarRocksRangePartitioner.PartitionRangeKey> partitionRangeKeys = new ArrayList<>();
        List<Class> partitionSchema = new ArrayList<>();
        partitionSchema.add(Integer.class);
        List<Integer> partitionKeyIndexes = new ArrayList<>();
        partitionKeyIndexes.add(0);
        StarRocksRangePartitioner rangePartitioner = new StarRocksRangePartitioner(partitionInfo, partitionKeyIndexes, null);
        int num = rangePartitioner.numPartitions();
        Assertions.assertEquals(1, num);

        List<Object> fields = new ArrayList<>();
        fields.add(100);
        fields.add("name");
        DppColumns record = new DppColumns(fields);
        int id = rangePartitioner.getPartition(record);
        Assertions.assertEquals(0, id);
    }
}
