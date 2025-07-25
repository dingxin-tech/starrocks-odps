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

import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Partition;
import com.starrocks.common.FeConstants;
import com.starrocks.connector.hive.HiveMetaClient;
import com.starrocks.connector.hive.MockedHiveMetadata;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.sql.plan.PlanTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class MvRewritePartialPartitionTest extends MVTestBase {
    private static MockedHiveMetadata mockedHiveMetadata;

    @BeforeAll
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();

        starRocksAssert.withTable(cluster, "t1");
        starRocksAssert.withTable(cluster, "test_base_part");
        starRocksAssert.withTable(cluster, "table_with_partition");
        starRocksAssert.withTable(cluster, "table_with_day_partition");
        starRocksAssert.withTable(cluster, "table_with_datetime_partition");

        ConnectorPlanTestBase.mockHiveCatalog(connectContext);
        mockedHiveMetadata =
                (MockedHiveMetadata) connectContext.getGlobalStateMgr().getMetadataMgr().
                        getOptionalMetadata(MockedHiveMetadata.MOCKED_HIVE_CATALOG_NAME).get();
        mockedHiveMetadata.updatePartitions("partitioned_db", "lineitem_par",
                ImmutableList.of("l_shipdate=" + HiveMetaClient.PARTITION_NULL_VALUE));
    }

    @Test
    public void testPartialPartition1() throws Exception {
        createAndRefreshMv("create materialized view partial_mv" +
                        " partition by id_date" +
                        " distributed by hash(`t1a`)" +
                        " as" +
                        " select t1a, id_date, t1b from table_with_partition");
        // modify p1991 and make it outdated
        // so p1992 and p1993 are updated
        executeInsertSql(connectContext, "insert into table_with_partition partition(p1991)" +
                " values(\"varchar12\", '1991-03-01', 2, 1, 1)");

        String query = "select t1a, id_date, t1b from table_with_partition" +
                " where id_date >= '1993-02-01' and id_date < '1993-05-01'";
        String plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "partial_mv");

        String query2 = "select t1a, id_date, t1b from table_with_partition" +
                " where id_date >= '1992-01-01' and id_date < '1993-01-01'";
        String plan2 = getFragmentPlan(query2);
        PlanTestBase.assertContains(plan2, "partial_mv");
        PlanTestBase.assertNotContains(plan2, "PREDICATES:");

        dropMv("test", "partial_mv");
    }

    @Test
    public void testPartialPartition2() throws Exception {
        createAndRefreshMv("create materialized view partial_mv_2" +
                        " partition by id_date" +
                        " distributed by hash(`t1a`)" +
                        " as" +
                        " select t1a, id_date, t1b from table_with_partition where t1b > 100");
        executeInsertSql(connectContext, "insert into table_with_partition partition(p1991)" +
                " values(\"varchar12\", '1991-03-01', 2, 1, 1)");
        String query4 = "select t1a, id_date, t1b from table_with_partition" +
                " where t1b > 110 and id_date >= '1993-02-01' and id_date < '1993-05-01'";
        String plan4 = getFragmentPlan(query4);
        PlanTestBase.assertContains(plan4, "partial_mv_2");
        dropMv("test", "partial_mv_2");
    }

    @Test
    public void testPartialPartition3_1() throws Exception {
        createAndRefreshMv("create materialized view partial_mv_3_1" +
                        " partition by date_trunc('month', new_date)" +
                        " distributed by hash(`t1a`)" +
                        " as" +
                        " select t1a, id_date as new_date, t1b from table_with_day_partition");
        executeInsertSql(connectContext, "insert into table_with_day_partition partition(p19910331)" +
                " values(\"varchar12\", '1991-03-31', 2, 2, 1)");
        String query5 = "select t1a, id_date, t1b from table_with_day_partition" +
                " where id_date >= '1991-04-01' and id_date < '1991-04-03'";
        String plan5 = getFragmentPlan(query5);
        PlanTestBase.assertContains(plan5, "partial_mv_3_1");
        dropMv("test", "partial_mv_3_1");
    }

    @Test
    public void testPartialPartition3_2() throws Exception {
        createAndRefreshMv("create materialized view partial_mv_3_2" +
                        " partition by new_date" +
                        " distributed by hash(`t1a`)" +
                        " as" +
                        " select t1a, id_date, date_trunc('month', id_date) as new_date, t1b " +
                        " from table_with_day_partition");
        executeInsertSql(connectContext, "insert into table_with_day_partition partition(p19910331)" +
                " values(\"varchar12\", '1991-03-31', 2, 2, 1)");
        String query6 = "select t1a, date_trunc('month', id_date), t1b from table_with_day_partition" +
                " where id_date >= '1991-04-01' and id_date < '1991-04-03'";
        String plan6 = getFragmentPlan(query6);
        PlanTestBase.assertContains(plan6, "partial_mv_3_2");
        dropMv("test", "partial_mv_3_2");
    }

    @Test
    public void testPartialPartition4() throws Exception {
        createAndRefreshMv("create materialized view partial_mv_4" +
                        " partition by new_name" +
                        " distributed by hash(`t1a`)" +
                        " as" +
                        " select t1a, id_date as new_name, t1b from table_with_partition");
        executeInsertSql(connectContext, "insert into table_with_partition partition(p1991)" +
                " values(\"varchar12\", '1991-03-01', 2, 1, 1)");
        String query7 = "select t1a, id_date, t1b from table_with_partition" +
                " where id_date >= '1993-02-01' and id_date < '1993-05-01'";
        String plan7 = getFragmentPlan(query7);
        PlanTestBase.assertContains(plan7, "partial_mv_4");
        dropMv("test", "partial_mv_4");
    }

    @Test
    public void testPartialPartition5() throws Exception {
        executeInsertSql(connectContext, "insert into test_base_part values (1, 1, 1, 1);");
        createAndRefreshMv("create materialized view partial_mv_5" +
                " partition by c3" +
                " distributed by hash(c1) as" +
                " select c1, c3, sum(c2) as c2 from test_base_part group by c1, c3;");
        executeInsertSql(connectContext, "alter table test_base_part add partition p6 values less than (\"4000\")");
        executeInsertSql(connectContext, "insert into test_base_part partition(p6) values (1, 2, 4500, 4)");
        String query8 = "select c3, sum(c2) from test_base_part group by c3";
        String plan8 = getFragmentPlan(query8);
        PlanTestBase.assertContains(plan8, "partial_mv_5");
        PlanTestBase.assertContains(plan8, "UNION");
        PlanTestBase.assertNotContains(plan8, "c3 < -9223372036854775808");

        String query9 = "select sum(c3) from test_base_part";
        String plan9 = getFragmentPlan(query9);
        PlanTestBase.assertNotContains(plan9, "partial_mv_5");
        dropMv("test", "partial_mv_5");
    }

    @Test
    public void testPartialPartition6() throws Exception {
        // test partition prune
        createAndRefreshMv("create materialized view partial_mv_6" +
                " partition by c3" +
                " distributed by hash(c1) as" +
                " select c1, c3, c2 from test_base_part where c3 < 2000;");
        String query10 = "select c1, c3, c2 from test_base_part";
        String plan10 = getFragmentPlan(query10);
        PlanTestBase.assertContains(plan10, "partial_mv_6", "UNION", "TABLE: test_base_part\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: (10: c3 >= 2000) OR (10: c3 IS NULL)");

        String query12 = "select c1, c3, c2 from test_base_part where c3 < 2000";
        String plan12 = getFragmentPlan(query12);
        PlanTestBase.assertContains(plan12, "partial_mv_6");

        String query13 = "select c1, c3, c2 from test_base_part where c3 < 1000";
        String plan13 = getFragmentPlan(query13);
        PlanTestBase.assertContains(plan13, "partial_mv_6", "PREDICATES: 6: c3 < 1000");
        dropMv("test", "partial_mv_6");
    }

    @Test
    public void testPartialPartition7() throws Exception {
        // test bucket prune
        createAndRefreshMv("create materialized view partial_mv_7" +
                " partition by c3" +
                " distributed by hash(c1) as" +
                " select c1, c3, c2 from test_base_part where c3 < 2000 and c1 = 1;");
        String query11 = "select c1, c3, c2 from test_base_part";
        String plan11 = getFragmentPlan(query11);
        PlanTestBase.assertContains(plan11, "partial_mv_7", "UNION", "TABLE: test_base_part");
        dropMv("test", "partial_mv_7");
    }

    @Test
    public void testPartialPartition8() throws Exception {
        createAndRefreshMv("create materialized view partial_mv_8" +
                " partition by c3" +
                " distributed by hash(c1) as" +
                " select c1, c3, c2 from test_base_part where c3 < 1000;");
        String query14 = "select c1, c3, c2 from test_base_part where c3 < 1000";
        String plan14 = getFragmentPlan(query14);
        PlanTestBase.assertContains(plan14, "partial_mv_8");
        dropMv("test", "partial_mv_8");
    }

    @Test
    public void testPartialPartition9() throws Exception {
        createAndRefreshMv("CREATE MATERIALIZED VIEW partial_mv_9" +
                " PARTITION BY k1 DISTRIBUTED BY HASH(k1) BUCKETS 10\n" +
                "REFRESH MANUAL AS SELECT k1, v1 as k2, v2 as k3 from t1;");
        // create nested mv based on partial_mv_9
        createAndRefreshMv("CREATE MATERIALIZED VIEW partial_mv_10" +
                " PARTITION BY k1 DISTRIBUTED BY HASH(k1) BUCKETS 10\n" +
                "REFRESH MANUAL AS SELECT k1, count(k2) as count_k2, sum(k3) as sum_k3 from partial_mv_9 group by k1;");
        executeInsertSql(connectContext, "insert into t1 values (4,1,1);");

        // first refresh nest mv partial_mv_10, will do nothing
        refreshMaterializedView("test", "partial_mv_10");
        // then refresh mv partial_mv_9
        refreshMaterializedView("test", "partial_mv_9");
        String query15 = "SELECT k1, count(v1), sum(v2) from t1 group by k1";
        String plan15 = getFragmentPlan(query15);
        // it should be union
        PlanTestBase.assertContains(plan15, "partial_mv_9");
        PlanTestBase.assertNotContains(plan15, "partial_mv_10");
        dropMv("test", "partial_mv_9");
        dropMv("test", "partial_mv_10");
    }

    @Test
    public void testPartialPartition10() throws Exception {
        starRocksAssert.withTable("CREATE TABLE ttl_base_table (\n" +
                "                            k1 INT,\n" +
                "                            v1 INT,\n" +
                "                            v2 INT)\n" +
                "                        DUPLICATE KEY(k1)\n" +
                "                        PARTITION BY RANGE(`k1`)\n" +
                "                        (\n" +
                "                        PARTITION `p1` VALUES LESS THAN ('2'),\n" +
                "                        PARTITION `p2` VALUES LESS THAN ('3'),\n" +
                "                        PARTITION `p3` VALUES LESS THAN ('4'),\n" +
                "                        PARTITION `p4` VALUES LESS THAN ('5'),\n" +
                "                        PARTITION `p5` VALUES LESS THAN ('6'),\n" +
                "                        PARTITION `p6` VALUES LESS THAN ('7')\n" +
                "                        )\n" +
                "                        DISTRIBUTED BY HASH(k1) properties('replication_num'='1');");
        executeInsertSql(connectContext, "insert into ttl_base_table values (1,1,1),(1,1,2),(1,2,1),(1,2,2),\n" +
                " (2,1,1),(2,1,2),(2,2,1),(2,2,2),\n" +
                " (3,1,1),(3,1,2),(3,2,1),(3,2,2);");
        createAndRefreshMv("CREATE MATERIALIZED VIEW ttl_mv_2\n" +
                " PARTITION BY k1\n" +
                " DISTRIBUTED BY HASH(k1) BUCKETS 10\n" +
                " REFRESH ASYNC\n" +
                " PROPERTIES(\n" +
                " 'partition_ttl_number'='4' \n" +
                " )\n" +
                "AS SELECT k1, sum(v1) as sum_v1 FROM ttl_base_table group by k1;");
        MaterializedView ttlMv2 = getMv("test", "ttl_mv_2");
        GlobalStateMgr.getCurrentState().getDynamicPartitionScheduler().runOnceForTest();
        Assertions.assertEquals(4, ttlMv2.getPartitions().size());

        String query16 = "select k1, sum(v1) FROM ttl_base_table where k1=3 group by k1";
        String plan16 = getFragmentPlan(query16);
        PlanTestBase.assertContains(plan16, "ttl_mv_2");
        dropMv("test", "ttl_mv_2");
        starRocksAssert.dropTable("ttl_base_table");
    }

    @Test
    public void testPartialPartition11() throws Exception {
        starRocksAssert.withTable("CREATE TABLE ttl_base_table_2 (\n" +
                "                            k1 date,\n" +
                "                            v1 INT,\n" +
                "                            v2 INT)\n" +
                "                        DUPLICATE KEY(k1)\n" +
                "                        PARTITION BY RANGE(`k1`)\n" +
                "                        (\n" +
                "                        PARTITION `p1` VALUES LESS THAN ('2020-01-01'),\n" +
                "                        PARTITION `p2` VALUES LESS THAN ('2020-02-01'),\n" +
                "                        PARTITION `p3` VALUES LESS THAN ('2020-03-01'),\n" +
                "                        PARTITION `p4` VALUES LESS THAN ('2020-04-01'),\n" +
                "                        PARTITION `p5` VALUES LESS THAN ('2020-05-01'),\n" +
                "                        PARTITION `p6` VALUES LESS THAN ('2020-06-01')\n" +
                "                        )\n" +
                "                        DISTRIBUTED BY HASH(k1) properties('replication_num'='1');");
        executeInsertSql(connectContext, "insert into ttl_base_table_2 values " +
                " (\"2019-01-01\",1,1),(\"2019-01-01\",1,2),(\"2019-01-01\",2,1),(\"2019-01-01\",2,2),\n" +
                " (\"2020-01-11\",1,1),(\"2020-01-11\",1,2),(\"2020-01-11\",2,1),(\"2020-01-11\",2,2),\n" +
                " (\"2020-02-11\",1,1),(\"2020-02-11\",1,2),(\"2020-02-11\",2,1),(\"2020-02-11\",2,2);");
        createAndRefreshMv("CREATE MATERIALIZED VIEW ttl_mv_3\n" +
                "               PARTITION BY k1\n" +
                "               DISTRIBUTED BY HASH(k1) BUCKETS 10\n" +
                "               REFRESH MANUAL\n" +
                "               AS SELECT k1, sum(v1) as sum_v1 FROM ttl_base_table_2 group by k1;");
        String query17 = "select k1, sum(v1) FROM ttl_base_table_2 where k1 = '2020-02-11' group by k1";
        String plan17 = getFragmentPlan(query17);
        PlanTestBase.assertContains(plan17, "ttl_mv_3", "k1 = '2020-02-11'");
        dropMv("test", "ttl_mv_3");
        starRocksAssert.dropTable("ttl_base_table_2");
    }

    @Test
    public void testPartitionTTL() throws Exception {
        starRocksAssert.withTable("CREATE TABLE ttl_base_table (\n" +
                "                            k1 INT,\n" +
                "                            v1 INT,\n" +
                "                            v2 INT)\n" +
                "                        DUPLICATE KEY(k1)\n" +
                "                        PARTITION BY RANGE(`k1`)\n" +
                "                        (\n" +
                "                        PARTITION `p1` VALUES LESS THAN ('2'),\n" +
                "                        PARTITION `p2` VALUES LESS THAN ('3'),\n" +
                "                        PARTITION `p3` VALUES LESS THAN ('4'),\n" +
                "                        PARTITION `p4` VALUES LESS THAN ('5'),\n" +
                "                        PARTITION `p5` VALUES LESS THAN ('6'),\n" +
                "                        PARTITION `p6` VALUES LESS THAN ('7')\n" +
                "                        )\n" +
                "                        DISTRIBUTED BY HASH(k1) properties('replication_num'='1');");
        executeInsertSql(connectContext, "insert into ttl_base_table values (1,1,1),(1,1,2),(1,2,1),(1,2,2),\n" +
                "(2,1,1),(2,1,2),(2,2,1),(2,2,2),\n" +
                "(3,1,1),(3,1,2),(3,2,1),(3,2,2);");

        String mvName = "ttl_mv_3";
        createAndRefreshMv("CREATE MATERIALIZED VIEW " + mvName +
                " PARTITION BY k1\n" +
                " DISTRIBUTED BY HASH(k1) BUCKETS 10\n" +
                " REFRESH ASYNC\n" +
                " PROPERTIES(\n" +
                " 'partition_refresh_number'='1',\n" +
                " 'partition_ttl_number'='2' \n" +
                " )\n" +
                " AS SELECT k1, sum(v1) as sum_v1 FROM ttl_base_table group by k1;");
        MaterializedView mv = getMv("test", mvName);

        // initial mv should create only 1 partition
        Assertions.assertEquals(2, mv.getPartitions().size());

        // refresh multiple times, should not change the live partition number
        for (int i = 0; i < 10; i++) {
            refreshMaterializedView("test", mvName);
            Assertions.assertEquals(2, mv.getPartitions().size(), "refresh " + i);
        }

        // increase the ttl number, and add more ttl partitions
        executeInsertSql(connectContext, String.format("alter materialized view %s set('partition_ttl_number'='5')", mvName));
        refreshMaterializedView("test", mvName);
        GlobalStateMgr.getCurrentState().getDynamicPartitionScheduler().runOnceForTest();
        Assertions.assertEquals(5, mv.getPartitions().size());

        // decrease the ttl number, and drop some ttl partitions
        executeInsertSql(connectContext, String.format("alter materialized view %s set('partition_ttl_number'='1')", mvName));
        refreshMaterializedView("test", mvName);
        GlobalStateMgr.getCurrentState().getDynamicPartitionScheduler().runOnceForTest();
        Assertions.assertEquals(1, mv.getPartitions().size());

        // cleanup
        dropMv("test", mvName);
        starRocksAssert.dropTable("ttl_base_table");
    }

    @Test
    public void testHivePartialPartitionWithTTL() throws Exception {
        starRocksAssert.getCtx().getSessionVariable().setEnableMaterializedViewUnionRewrite(true);
        createAndRefreshMv("CREATE MATERIALIZED VIEW `hive_parttbl_mv`\n" +
                        "PARTITION BY (`l_shipdate`)\n" +
                        "DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 10\n" +
                        "REFRESH MANUAL\n" +
                        "PROPERTIES (\n" +
                        "\"partition_ttl_number\" = \"3\"\n" +
                        ")\n" +
                        "AS SELECT `l_orderkey`, `l_suppkey`, `l_shipdate`  FROM `hive0`.`partitioned_db`.`lineitem_par` as a;");

        MaterializedView ttlMv = getMv("test", "hive_parttbl_mv");
        GlobalStateMgr.getCurrentState().getDynamicPartitionScheduler().runOnceForTest();
        Assertions.assertEquals(3, ttlMv.getPartitions().size());

        String query = "SELECT `l_orderkey`, `l_suppkey`, `l_shipdate`  FROM `hive0`.`partitioned_db`.`lineitem_par`";
        String plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "0:UNION");

        query = "SELECT `l_orderkey`, `l_suppkey`, `l_shipdate`  FROM `hive0`.`partitioned_db`.`lineitem_par` " +
                "where l_shipdate = '1998-01-01'";
        plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "HdfsScanNode");

        query = "SELECT `l_orderkey`, `l_suppkey`, `l_shipdate`  FROM `hive0`.`partitioned_db`.`lineitem_par` " +
                "where l_shipdate >= '1998-01-04'";
        plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "hive_parttbl_mv");

        dropMv("test", "hive_parttbl_mv");
    }

    @Test
    public void testHivePartialPartition1() throws Exception {
        starRocksAssert.getCtx().getSessionVariable().setEnableMaterializedViewUnionRewrite(true);
        createAndRefreshMv("CREATE MATERIALIZED VIEW `hive_parttbl_mv`\n" +
                        "PARTITION BY (`l_shipdate`)\n" +
                        "DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 10\n" +
                        "REFRESH MANUAL\n" +
                        "AS SELECT `l_orderkey`, `l_suppkey`, `l_shipdate`  FROM `hive0`.`partitioned_db`.`lineitem_par` as a;");

        String query = "SELECT `l_orderkey`, `l_suppkey`, `l_shipdate`  FROM `hive0`.`partitioned_db`.`lineitem_par`";
        String plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "hive_parttbl_mv");

        query = "SELECT `l_orderkey`, `l_suppkey`, `l_shipdate`  FROM `hive0`.`partitioned_db`.`lineitem_par` " +
                "where l_shipdate > '1998-01-04'";
        plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "hive_parttbl_mv");

        query = "SELECT `l_orderkey`, `l_suppkey`, `l_shipdate`  FROM `hive0`.`partitioned_db`.`lineitem_par` " +
                "where l_shipdate > '1998-01-04' and l_shipdate < '1998-01-06'";
        plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "hive_parttbl_mv");

        query = "SELECT `l_orderkey`, `l_suppkey`, `l_shipdate`  FROM `hive0`.`partitioned_db`.`lineitem_par` ";
        plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "TABLE: hive_parttbl_mv\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=6/6");
        dropMv("test", "hive_parttbl_mv");
    }

    @Test
    public void testHivePartialPartition2() throws Exception {
        createAndRefreshMv("CREATE MATERIALIZED VIEW `hive_parttbl_mv_2`\n" +
                        "PARTITION BY (`l_shipdate`)\n" +
                        "DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 10\n" +
                        "REFRESH MANUAL\n" +
                        "AS SELECT `l_orderkey`, `l_suppkey`, `l_shipdate`  FROM `hive0`.`partitioned_db`.`lineitem_par` " +
                        "where l_orderkey > 100;");
        String query = "SELECT `l_orderkey`, `l_suppkey`, `l_shipdate`  FROM `hive0`.`partitioned_db`.`lineitem_par` " +
                "where l_orderkey > 100;";
        String plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "hive_parttbl_mv_2");

        mockedHiveMetadata.updatePartitions("partitioned_db", "lineitem_par",
                ImmutableList.of("l_shipdate=1998-01-02"));
        plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "     TABLE: hive_parttbl_mv_2\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=5/6\n" +
                "     rollup: hive_parttbl_mv_2");
        PlanTestBase.assertContains(plan, "     TABLE: lineitem_par\n" +
                "     PARTITION PREDICATES: 25: l_shipdate IN ('1998-01-02')\n" +
                "     NON-PARTITION PREDICATES: 23: l_orderkey > 100\n" +
                "     MIN/MAX PREDICATES: 23: l_orderkey > 100\n" +
                "     partitions=1/6");
        dropMv("test", "hive_parttbl_mv_2");
    }

    @Test
    public void testHivePartialPartition3() throws Exception {
        // test partition prune
        createAndRefreshMv("CREATE MATERIALIZED VIEW `hive_parttbl_mv_3`\n" +
                        "PARTITION BY (`l_shipdate`)\n" +
                        "DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 10\n" +
                        "REFRESH MANUAL\n" +
                        "AS SELECT `l_orderkey`, `l_suppkey`, `l_shipdate`  FROM `hive0`.`partitioned_db`.`lineitem_par` " +
                        "where l_shipdate > '1998-01-02';");
        String query = "SELECT `l_orderkey`, `l_suppkey`, `l_shipdate`  FROM `hive0`.`partitioned_db`.`lineitem_par` ";
        String plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "hive_parttbl_mv_3", "partitions=3/6", "lineitem_par");
        PlanTestBase.assertNotContains(plan, "partitions=2/6");
        dropMv("test", "hive_parttbl_mv_3");
    }

    @Test
    public void testHivePartialPartition4() throws Exception {
        createAndRefreshMv("CREATE MATERIALIZED VIEW `hive_parttbl_mv_4`\n" +
                        "PARTITION BY (`l_shipdate`)\n" +
                        "DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 10\n" +
                        "REFRESH MANUAL\n" +
                        "AS SELECT `l_orderkey`, `l_suppkey`, `l_shipdate`  FROM `hive0`.`partitioned_db`.`lineitem_par` " +
                        "where l_shipdate < '1998-01-02' and l_orderkey = 100;");
        String query = "SELECT `l_orderkey`, `l_suppkey`, `l_shipdate`  FROM `hive0`.`partitioned_db`.`lineitem_par` ";
        String plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "     TABLE: lineitem_par\n" +
                        "     NON-PARTITION PREDICATES: (((22: l_shipdate < '1998-01-02') AND (20: l_orderkey = 100) IS NULL) " +
                        "OR (22: l_shipdate >= '1998-01-02')) OR ((22: l_shipdate IS NULL) OR (20: l_orderkey != 100))\n" +
                        "     partitions=6/6",
                "     TABLE: hive_parttbl_mv_4\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     partitions=2/6");
        dropMv("test", "hive_parttbl_mv_4");
    }

    @Test
    public void testHivePartialPartition5() throws Exception {
        connectContext.getSessionVariable().setEnableMaterializedViewTransparentUnionRewrite(false);
        createAndRefreshMv("CREATE MATERIALIZED VIEW `hive_parttbl_mv_5`\n" +
                        "PARTITION BY date_trunc('month', o_orderdate)\n" +
                        "DISTRIBUTED BY HASH(`o_orderkey`) BUCKETS 10\n" +
                        "REFRESH MANUAL\n" +
                        "AS SELECT `o_orderkey`, `o_orderstatus`, `o_orderdate`  FROM `hive0`.`partitioned_db`.`orders`");

        String query = "SELECT `o_orderkey`, `o_orderstatus`, `o_orderdate`  FROM `hive0`.`partitioned_db`.`orders`";
        String plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "hive_parttbl_mv_5", "360/360");

        query = "SELECT `o_orderkey`, `o_orderstatus`, `o_orderdate`  FROM `hive0`.`partitioned_db`.`orders` " +
                "where o_orderdate >= '1991-01-01' and o_orderdate < '1991-02-1'";
        plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "hive_parttbl_mv_5", "partitions=1/36");
        mockedHiveMetadata.updatePartitions("partitioned_db", "orders",
                ImmutableList.of("o_orderdate=1991-02-02"));

        query = "SELECT `o_orderkey`, `o_orderstatus`, `o_orderdate`  FROM `hive0`.`partitioned_db`.`orders` ";
        plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "hive_parttbl_mv_5", "orders",
                "     partitions=28/1095");

        // updated partitions are not in the query's partition range
        query = "SELECT `o_orderkey`, `o_orderstatus`, `o_orderdate`  FROM `hive0`.`partitioned_db`.`orders` " +
                "where o_orderdate >= '1992-05-01' and o_orderdate < '1992-05-31'";
        plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "PREDICATES: 12: o_orderdate < '1992-05-31'\n" +
                "     partitions=1/36\n" +
                "     rollup: hive_parttbl_mv_5");

        refreshMaterializedView("test", "hive_parttbl_mv_5");
        mockedHiveMetadata.updatePartitions("partitioned_db", "orders",
                ImmutableList.of("o_orderdate=1991-01-02"));
        query = "SELECT `o_orderkey`, `o_orderstatus`, `o_orderdate`  FROM `hive0`.`partitioned_db`.`orders` " +
                "where o_orderdate >= '1992-05-01' and o_orderdate < '1992-05-31'";
        plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "hive_parttbl_mv_5", "PREDICATES: 12: o_orderdate < '1992-05-31'",
                "partitions=1/36");

        dropMv("test", "hive_parttbl_mv_5");
        connectContext.getSessionVariable().setEnableMaterializedViewTransparentUnionRewrite(true);
    }

    @Test
    public void testNullPartitionRewriteWithLoad() throws Exception {
        {
            executeInsertSql(connectContext, "insert into test_base_part values(1, 1, 2, 3),(100, 1, 2, 3),(200, 1, 2, 3)," +
                    "(1000, 1, 2, 3),(2000, 1, 2, 3),(2500, 1, 2, 3)");

            createAndRefreshMv("CREATE MATERIALIZED VIEW `partial_mv_12`\n" +
                    "PARTITION BY (`c3`)\n" +
                    "DISTRIBUTED BY HASH(`c1`) BUCKETS 6\n" +
                    "REFRESH MANUAL\n" +
                    "AS SELECT `c1`, `c3`, sum(`c4`) AS `total`\n" +
                    " FROM `test_base_part` WHERE `c3` is null GROUP BY `c3`, `c1`;");

            String query = "select c1, c3, sum(c4) from test_base_part group by c1, c3;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "partial_mv_12", "PREDICATES: 10: c3 IS NOT NULL");
            starRocksAssert.dropMaterializedView("partial_mv_12");
        }

        {
            executeInsertSql(connectContext, "insert into test_base_part values(1, 1, 2, 3),(100, 1, 2, 3),(200, 1, 2, 3)," +
                    "(1000, 1, 2, 3),(2000, 1, 2, 3),(2500, 1, 2, 3)");

            createAndRefreshMv("CREATE MATERIALIZED VIEW `partial_mv_13`\n" +
                    "PARTITION BY (`c3`)\n" +
                    "DISTRIBUTED BY HASH(`c1`) BUCKETS 6\n" +
                    "REFRESH MANUAL\n" +
                    "AS SELECT `c1`, `c3`, sum(`c4`) AS `total`\n" +
                    "FROM `test_base_part` WHERE `c3` is null GROUP BY `c3`, `c1`;");

            // test update for null partition
            executeInsertSql(connectContext, "insert into test_base_part values(null, 1, null, 3)");

            String query = "select c1, c3, sum(c4) from test_base_part group by c1, c3;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertNotContains(plan, "partial_mv_13");
            starRocksAssert.dropMaterializedView("partial_mv_13");
        }
    }

    @Test
    public void testHivePartitionQueryRewrite1() throws Exception {
        starRocksAssert.getCtx().getSessionVariable().setEnableMaterializedViewUnionRewrite(true);
        String mvName = "hive_query_rewrite";

        // Disable
        createAndRefreshMv("CREATE MATERIALIZED VIEW `hive_query_rewrite`\n" +
                        "DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 10\n" +
                        "REFRESH MANUAL\n" +
                        "AS SELECT `l_orderkey`, `l_suppkey`, `l_shipdate`  FROM `hive0`.`partitioned_db`.`lineitem_par` as a;");

        MaterializedView ttlMv = getMv("test", mvName);
        GlobalStateMgr.getCurrentState().getDynamicPartitionScheduler().runOnceForTest();
        Assertions.assertEquals(1, ttlMv.getPartitions().size());

        String query = "SELECT `l_orderkey`, `l_suppkey`, `l_shipdate`  FROM `hive0`.`partitioned_db`.`lineitem_par` " +
                "where l_shipdate >= '1998-01-04'";
        PlanTestBase.assertContains(getFragmentPlan(query), mvName);
        dropMv("test", mvName);

        // Checked
        createAndRefreshMv("CREATE MATERIALIZED VIEW `hive_query_rewrite`\n" +
                        "DISTRIBUTED BY HASH(`l_shipdate`) BUCKETS 10\n" +
                        "REFRESH DEFERRED MANUAL\n" +
                        "AS SELECT `l_shipdate`, sum(`l_orderkey`)  FROM `hive0`.`partitioned_db`.`lineitem_par` " +
                        "GROUP BY l_shipdate");

        query = "SELECT `l_orderkey`, `l_suppkey`, `l_shipdate`  FROM `hive0`.`partitioned_db`.`lineitem_par` " +
                "where l_shipdate >= '1998-01-04'";
        PlanTestBase.assertNotContains(getFragmentPlan(query), mvName);
        // refresh mv
        refreshMaterializedView("test", mvName);
        query =
                "SELECT `l_shipdate`, sum(`l_orderkey`)  FROM `hive0`.`partitioned_db`.`lineitem_par` GROUP BY l_shipdate";
        PlanTestBase.assertContains(getFragmentPlan(query), mvName);
        dropMv("test", mvName);

        // Loose
        createAndRefreshMv("CREATE MATERIALIZED VIEW `hive_query_rewrite`\n" +
                        "DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 10\n" +
                        "REFRESH DEFERRED MANUAL\n" +
                        "AS SELECT `l_orderkey`, `l_suppkey`, `l_shipdate`  FROM `hive0`.`partitioned_db`.`lineitem_par` as a;");

        query = "SELECT `l_orderkey`, `l_suppkey`, `l_shipdate`  FROM `hive0`.`partitioned_db`.`lineitem_par` " +
                "where l_shipdate >= '1998-01-04'";
        PlanTestBase.assertContains(getFragmentPlan(query), mvName);
        // refresh mv
        refreshMaterializedView("test", mvName);
        query = "SELECT `l_orderkey`, `l_suppkey`, `l_shipdate`  FROM `hive0`.`partitioned_db`.`lineitem_par` " +
                "where l_shipdate >= '1998-01-04'";
        PlanTestBase.assertContains(getFragmentPlan(query), mvName);
        dropMv("test", mvName);
    }

    @Test
    public void testHivePartitionQueryRewrite2() throws Exception {
        starRocksAssert.getCtx().getSessionVariable().setEnableMaterializedViewUnionRewrite(true);
        String mvName = "hive_query_rewrite";

        {
            // default
            createAndRefreshMv("CREATE MATERIALIZED VIEW `hive_query_rewrite`\n" +
                            "DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 10\n" +
                            "REFRESH MANUAL\n" +
                            "AS SELECT `l_orderkey`, `l_suppkey`, `l_shipdate`  " +
                            "FROM `hive0`.`partitioned_db`.`lineitem_par` as a;");

            MaterializedView ttlMv = getMv("test", mvName);
            GlobalStateMgr.getCurrentState().getDynamicPartitionScheduler().runOnceForTest();
            Assertions.assertEquals(1, ttlMv.getPartitions().size());

            String query = "SELECT `l_orderkey`, `l_suppkey`, `l_shipdate`  FROM `hive0`.`partitioned_db`.`lineitem_par` " +
                    "where l_shipdate >= '1998-01-04'";
            PlanTestBase.assertContains(getFragmentPlan(query), mvName);
            dropMv("test", mvName);
        }

        {
            // Disable
            createAndRefreshMv("CREATE MATERIALIZED VIEW `hive_query_rewrite`\n" +
                            "DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 10\n" +
                            "REFRESH MANUAL\n" +
                            "AS SELECT `l_orderkey`, `l_suppkey`, `l_shipdate`  " +
                            "FROM `hive0`.`partitioned_db`.`lineitem_par` as a;");

            MaterializedView ttlMv = getMv("test", mvName);
            GlobalStateMgr.getCurrentState().getDynamicPartitionScheduler().runOnceForTest();
            Assertions.assertEquals(1, ttlMv.getPartitions().size());

            String query = "SELECT `l_orderkey`, `l_suppkey`, `l_shipdate`  FROM `hive0`.`partitioned_db`.`lineitem_par` " +
                    "where l_shipdate >= '1998-01-04'";
            PlanTestBase.assertContains(getFragmentPlan(query), mvName);
            dropMv("test", mvName);
        }

        {
            // Checked
            createAndRefreshMv("CREATE MATERIALIZED VIEW `hive_query_rewrite`\n" +
                    "DISTRIBUTED BY HASH(`l_shipdate`) BUCKETS 10\n" +
                    "REFRESH DEFERRED MANUAL\n" +
                    "AS SELECT `l_shipdate`, sum(`l_orderkey`)  FROM `hive0`.`partitioned_db`.`lineitem_par` " +
                    "GROUP BY l_shipdate");

            String query = "SELECT `l_orderkey`, `l_suppkey`, `l_shipdate`  FROM `hive0`.`partitioned_db`.`lineitem_par` " +
                    "where l_shipdate >= '1998-01-04'";
            PlanTestBase.assertNotContains(getFragmentPlan(query), mvName);
            // refresh mv
            refreshMaterializedView("test", mvName);
            query =
                    "SELECT `l_shipdate`, sum(`l_orderkey`)  FROM `hive0`.`partitioned_db`.`lineitem_par` GROUP BY l_shipdate";
            PlanTestBase.assertContains(getFragmentPlan(query), mvName);
            dropMv("test", mvName);

        }
        {
            // Loose
            createAndRefreshMv("CREATE MATERIALIZED VIEW `hive_query_rewrite`\n" +
                            "DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 10\n" +
                            "REFRESH DEFERRED MANUAL\n" +
                            "AS SELECT `l_orderkey`, `l_suppkey`, `l_shipdate`  " +
                            "FROM `hive0`.`partitioned_db`.`lineitem_par` as a;");

            String query = "SELECT `l_orderkey`, `l_suppkey`, `l_shipdate`  FROM `hive0`.`partitioned_db`.`lineitem_par` " +
                    "where l_shipdate >= '1998-01-04'";
            PlanTestBase.assertContains(getFragmentPlan(query), mvName);
            // refresh mv
            refreshMaterializedView("test", mvName);
            query = "SELECT `l_orderkey`, `l_suppkey`, `l_shipdate`  FROM `hive0`.`partitioned_db`.`lineitem_par` " +
                    "where l_shipdate >= '1998-01-04'";
            PlanTestBase.assertContains(getFragmentPlan(query), mvName);
            dropMv("test", mvName);
        }
    }

    @Test
    public void testPartitionQueryRewriteSkipEmptyPartitions() throws Exception {
        starRocksAssert.getCtx().getSessionVariable().setEnableMaterializedViewUnionRewrite(true);
        {
            // Loose, empty mv partitions should be skipped
            starRocksAssert.withRefreshedMaterializedView("create materialized view test_loose_mv" +
                    " partition by id_date" +
                    " distributed by random" +
                    " REFRESH ASYNC\n" +
                    " PROPERTIES (\n" +
                    "\"replication_num\" = \"1\",\n" +
                    "\"query_rewrite_consistency\" = \"loose\"," +
                    "\"auto_refresh_partitions_limit\" = \"1\"" +
                    ")\n" +
                    " as" +
                    " select id_date, sum(t1b) from table_with_day_partition group by id_date");
            MaterializedView mv = starRocksAssert.getMv("test", "test_loose_mv");
            mv.getPartition("p19910330").getDefaultPhysicalPartition()
                    .setVisibleVersion(Partition.PARTITION_INIT_VERSION, System.currentTimeMillis());
            String query5 = "select id_date, sum(t1b) from table_with_day_partition" +
                    " where id_date >= '1991-03-30' and id_date < '1991-04-03' group by id_date";
            FeConstants.runningUnitTest = false;
            String plan = getFragmentPlan(query5);
            FeConstants.runningUnitTest = true;
            PlanTestBase.assertContains(plan, "test_loose_mv", "partitions=3/4",
                    "table_with_day_partition", "partitions=1/4", "UNION");
            dropMv("test", "test_loose_mv");
        }
    }

    @Test
    public void testPartialPartitionRewriteWithDateTruncExpr1() throws Exception {
        starRocksAssert.withTable("CREATE TABLE base_tbl1 (\n" +
                " k1 datetime,\n" +
                " v1 INT,\n" +
                " v2 INT)\n" +
                " DUPLICATE KEY(k1)\n" +
                " PARTITION BY RANGE(`k1`)\n" +
                " (\n" +
                "  PARTITION `p1` VALUES LESS THAN ('2020-01-01'),\n" +
                "  PARTITION `p2` VALUES LESS THAN ('2020-02-01'),\n" +
                "  PARTITION `p3` VALUES LESS THAN ('2020-03-01')\n" +
                " )\n" +
                " DISTRIBUTED BY HASH(k1) properties('replication_num'='1');");
        executeInsertSql(connectContext, "insert into base_tbl1 values " +
                " (\"2020-01-01\",1,1),(\"2020-01-01\",1,2),(\"2020-01-11\",2,1),(\"2020-01-11\",2,2);");

        createAndRefreshMv("CREATE MATERIALIZED VIEW test_mv1 \n" +
                " PARTITION BY ds \n" +
                " DISTRIBUTED BY HASH(ds) BUCKETS 10\n" +
                " REFRESH MANUAL\n" +
                " AS SELECT " +
                " date_trunc('minute', `k1`) AS ds, sum(v1) " +
                " FROM base_tbl1 " +
                " group by ds;");

        {
            String query = "select date_trunc('minute', `k1`) AS ds, sum(v1) " +
                    " FROM base_tbl1 where date_trunc('minute', `k1`) = '2020-02-11' group by ds";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "test_mv1", "ds = '2020-02-11 00:00:00'");
        }

        {
            String query = "select date_trunc('minute', `k1`) AS ds, sum(v1) " +
                    " FROM base_tbl1 where date_trunc('minute', `k1`) = '2020-02-11' group by ds";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "test_mv1", "ds = '2020-02-11 00:00:00'");
        }

        {
            String query = "select date_trunc('minute', `k1`) AS ds, sum(v1) " +
                    " FROM base_tbl1 group by ds";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "test_mv1");
        }

        executeInsertSql(connectContext, "insert into base_tbl1 partition('p3') values (\"2020-02-02\",1,1)");
        {
            String query = "select date_trunc('minute', `k1`) AS ds, sum(v1) " +
                    " FROM base_tbl1 " +
                    " WHERE date_trunc('minute', `k1`) >= '2020-01-01 00:00:00'" +
                    " group by ds";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "TABLE: test_mv1\n" +
                    "     PREAGGREGATION: ON\n" +
                    "     PREDICATES: 8: ds >= '2020-01-01 00:00:00'\n" +
                    "     partitions=1/3");
            PlanTestBase.assertContains(plan, "     TABLE: base_tbl1\n" +
                    "     PREAGGREGATION: ON\n" +
                    "     PREDICATES: date_trunc('minute', 10: k1) >= '2020-01-01 00:00:00'\n" +
                    "     partitions=1/3");
        }

        {
            String query = "select date_trunc('minute', `k1`) AS ds, sum(v1) " +
                    " FROM base_tbl1 " +
                    " WHERE date_trunc('minute', `k1`) >= '2020-01-01 00:00:00' and " +
                    "   date_trunc('minute', `k1`) <= '2020-03-01 00:00:00' " +
                    " group by ds";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "     TABLE: base_tbl1\n" +
                    "     PREAGGREGATION: ON\n" +
                    "     PREDICATES: date_trunc('minute', 10: k1) >= '2020-01-01 00:00:00', " +
                    "date_trunc('minute', 10: k1) <= '2020-03-01 00:00:00'\n" +
                    "     partitions=1/3");
            PlanTestBase.assertContains(plan, "TABLE: test_mv1\n" +
                    "     PREAGGREGATION: ON\n" +
                    "     PREDICATES: 8: ds >= '2020-01-01 00:00:00', 8: ds <= '2020-03-01 00:00:00'\n" +
                    "     partitions=1/3");
        }

        dropMv("test", "test_mv1");
        starRocksAssert.dropTable("base_tbl1");
    }

    @Test
    public void testPartialPartitionRewriteWithDateTruncExpr2() throws Exception {
        starRocksAssert.withTable("CREATE TABLE base_tbl1 (\n" +
                " k1 datetime,\n" +
                " v1 INT,\n" +
                " v2 INT)\n" +
                " DUPLICATE KEY(k1)\n" +
                " PARTITION BY RANGE(`k1`)\n" +
                " (\n" +
                "  PARTITION `p1` VALUES [('2020-01-01') , ('2020-02-01')),\n" +
                "  PARTITION `p2` VALUES [('2020-02-01') , ('2020-03-01')),\n" +
                "  PARTITION `p3` VALUES [('2020-03-01') , ('2020-04-01'))\n" +
                " )\n" +
                " DISTRIBUTED BY HASH(k1) properties('replication_num'='1');");
        executeInsertSql(connectContext, "insert into base_tbl1 values " +
                " (\"2020-01-01\",1,1),(\"2020-01-01\",1,2),(\"2020-01-11\",2,1),(\"2020-01-11\",2,2);");

        createAndRefreshMv("CREATE MATERIALIZED VIEW test_mv1 \n" +
                " PARTITION BY ds \n" +
                " DISTRIBUTED BY HASH(ds) BUCKETS 10\n" +
                " REFRESH MANUAL\n" +
                " AS SELECT " +
                " date_trunc('minute', `k1`) AS ds, sum(v1) " +
                " FROM base_tbl1 " +
                " group by ds;");

        {
            String query = "select date_trunc('minute', `k1`) AS ds, sum(v1) " +
                    " FROM base_tbl1 where date_trunc('minute', `k1`) = '2020-02-11' group by ds";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "test_mv1", "ds = '2020-02-11 00:00:00'");
        }

        {
            String query = "select date_trunc('minute', `k1`) AS ds, sum(v1) " +
                    " FROM base_tbl1 where date_trunc('minute', `k1`)  = '2020-02-11' group by ds";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "test_mv1", "ds = '2020-02-11 00:00:00'");
        }

        {
            String query = "select date_trunc('minute', `k1`) AS ds, sum(v1) " +
                    " FROM base_tbl1 group by ds";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "test_mv1");
        }

        executeInsertSql(connectContext, "insert into base_tbl1 partition('p3') values (\"2020-02-02\",1,1)");
        {
            String query = "select date_trunc('minute', `k1`) AS ds, sum(v1) " +
                    " FROM base_tbl1 group by ds";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "test_mv1", "UNION");
        }

        {
            String query = "select date_trunc('minute', `k1`) AS ds, sum(v1) " +
                    " FROM base_tbl1 " +
                    " WHERE date_trunc('minute', `k1`) >= '2020-01-01 00:00:00' " +
                    " group by ds";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "test_mv1", "UNION");
        }

        {
            String query = "select date_trunc('minute', `k1`) AS ds, sum(v1) " +
                    " FROM base_tbl1 " +
                    " WHERE date_trunc('minute', `k1`) <= '2020-03-01 00:00:00' " +
                    " group by ds";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "test_mv1", "UNION");
        }

        {
            String query = "select date_trunc('minute', `k1`) AS ds, sum(v1) " +
                    " FROM base_tbl1 " +
                    " WHERE date_trunc('minute', `k1`) >= '2020-01-01 00:00:00' and " +
                    "   date_trunc('minute', `k1`) <= '2020-03-01 00:00:00' " +
                    " group by ds";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "test_mv1", "UNION");
        }

        dropMv("test", "test_mv1");
        starRocksAssert.dropTable("base_tbl1");
    }

    @Test
    public void testPartialPartitionRewriteWithDateTruncExpr3() throws Exception {
        starRocksAssert.withTable("CREATE TABLE base_tbl1 (\n" +
                " ds date,\n" +
                " v1 INT,\n" +
                " v2 INT)\n" +
                " DUPLICATE KEY(ds)\n" +
                " PARTITION BY RANGE(`ds`)\n" +
                " (\n" +
                "  PARTITION `p1` VALUES [('2020-01-01') , ('2020-01-02')),\n" +
                "  PARTITION `p2` VALUES [('2020-01-03') , ('2020-01-04')),\n" +
                "  PARTITION `p3` VALUES [('2020-02-02') , ('2020-02-03'))\n" +
                " )\n" +
                " DISTRIBUTED BY HASH(ds) properties('replication_num'='1');");
        executeInsertSql(connectContext, "insert into base_tbl1 values " +
                " (\"2020-01-01\",1,1),(\"2020-01-03\",1,2),(\"2020-02-02\",2,1);");

        createAndRefreshMv("CREATE MATERIALIZED VIEW test_month_mv1 \n" +
                " PARTITION BY ds \n" +
                " DISTRIBUTED BY HASH(ds) BUCKETS 10\n" +
                " REFRESH MANUAL\n" +
                " AS SELECT " +
                " date_trunc('month', `ds`) AS ds, sum(v1) " +
                " FROM base_tbl1 " +
                " group by ds;");

        {
            String query = "select sum(v1) " +
                    " FROM base_tbl1 where ds >= '2020-01-01' and ds <= '2020-01-31'";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "test_month_mv1");
        }

        {
            String query = "select sum(v1) " +
                    " FROM base_tbl1 where ds >= '2020-01-01' and ds < '2020-02-01'";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "test_month_mv1");
        }

        {
            String query = "select sum(v1) " +
                    " FROM base_tbl1 where ds >= '2020-01-01' and ds <= '2020-02-29'";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "test_month_mv1");
        }

        {
            String query = "select sum(v1) " +
                    " FROM base_tbl1 where ds >= '2020-01-01' and ds < '2020-03-01'";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "test_month_mv1");
        }

        dropMv("test", "test_mv1");
        starRocksAssert.dropTable("base_tbl1");
    }

    @Test
    public void testPartialPartitionRewriteWithTimeSliceExpr1() throws Exception {
        starRocksAssert.withTable("CREATE TABLE base_tbl1 (\n" +
                " k1 datetime,\n" +
                " v1 INT,\n" +
                " v2 INT)\n" +
                " DUPLICATE KEY(k1)\n" +
                " PARTITION BY RANGE(`k1`)\n" +
                " (\n" +
                "  PARTITION `p1` VALUES LESS THAN ('2020-01-01'),\n" +
                "  PARTITION `p2` VALUES LESS THAN ('2020-02-01'),\n" +
                "  PARTITION `p3` VALUES LESS THAN ('2020-03-01')\n" +
                " )\n" +
                " DISTRIBUTED BY HASH(k1) properties('replication_num'='1');");
        executeInsertSql(connectContext, "insert into base_tbl1 values " +
                " (\"2020-01-01\",1,1),(\"2020-01-01\",1,2),(\"2020-01-11\",2,1),(\"2020-01-11\",2,2);");

        createAndRefreshMv("CREATE MATERIALIZED VIEW test_mv1 \n" +
                " PARTITION BY date_trunc('day', ds) \n" +
                " DISTRIBUTED BY HASH(ds) BUCKETS 10\n" +
                " REFRESH MANUAL\n" +
                " AS SELECT " +
                " time_slice(k1, interval 1 hour) AS ds, sum(v1) " +
                " FROM base_tbl1 " +
                " group by ds;");

        {
            String query = "select time_slice(k1, interval 1 hour) AS ds, sum(v1) " +
                    " FROM base_tbl1 where time_slice(k1, interval 1 hour) = '2020-02-11 00:00:00' " +
                    "group by ds";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "test_mv1", "ds = '2020-02-11 00:00:00'");
        }

        {
            String query = "select time_slice(k1, interval 1 hour) AS ds, sum(v1) " +
                    " FROM base_tbl1 group by ds";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "test_mv1");
        }

        executeInsertSql(connectContext, "insert into base_tbl1 partition('p3') values (\"2020-02-02\",1,1)");
        {
            String query = "select time_slice(k1, interval 1 hour) AS ds, sum(v1) " +
                    " FROM base_tbl1 " +
                    " where time_slice(k1, interval 1 hour) >= '2020-02-11 00:00:00' " +
                    " group by ds";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "test_mv1");
        }

        {
            String query = "select time_slice(k1, interval 1 hour) AS ds, sum(v1) " +
                    " FROM base_tbl1 " +
                    " where time_slice(k1, interval 1 hour) >= '2020-02-11 00:00:00' " +
                    " and time_slice(k1, interval 1 hour) <= '2020-03-01 00:00:00' " +
                    " group by ds";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "test_mv1");
            // partition p2 has already been updated, so the mv should not be used anymore
            PlanTestBase.assertContains(plan, "     TABLE: test_mv1\n" +
                            "     PREAGGREGATION: ON\n" +
                            "     PREDICATES: 8: ds >= '2020-02-11 00:00:00', 8: ds <= '2020-03-01 00:00:00'\n" +
                            "     partitions=0/61");
            PlanTestBase.assertContains(plan, "     TABLE: base_tbl1\n" +
                    "     PREAGGREGATION: ON\n" +
                    "     PREDICATES: time_slice(10: k1, 1, 'hour', 'floor') >= '2020-02-11 00:00:00', " +
                    "time_slice(10: k1, 1, 'hour', 'floor') <= '2020-03-01 00:00:00'\n" +
                    "     partitions=1/3");
        }

        dropMv("test", "test_mv1");
        starRocksAssert.dropTable("base_tbl1");
    }

    @Test
    public void testPartialPartitionRewriteWiteTimeSliceExpr2() throws Exception {
        starRocksAssert.withTable("CREATE TABLE base_tbl1 (\n" +
                " k1 datetime,\n" +
                " v1 INT,\n" +
                " v2 INT)\n" +
                " DUPLICATE KEY(k1)\n" +
                " PARTITION BY RANGE(`k1`)\n" +
                " (\n" +
                "  PARTITION `p1` VALUES [('2020-01-01') , ('2020-02-01')),\n" +
                "  PARTITION `p2` VALUES [('2020-02-01') , ('2020-03-01')),\n" +
                "  PARTITION `p3` VALUES [('2020-03-01') , ('2020-04-01'))\n" +
                " )\n" +
                " DISTRIBUTED BY HASH(k1) properties('replication_num'='1');");
        executeInsertSql(connectContext, "insert into base_tbl1 values " +
                " (\"2020-01-01\",1,1),(\"2020-01-01\",1,2),(\"2020-01-11\",2,1),(\"2020-01-11\",2,2);");

        createAndRefreshMv("CREATE MATERIALIZED VIEW test_mv1 \n" +
                " PARTITION BY date_trunc('day', ds) \n" +
                " DISTRIBUTED BY HASH(ds) BUCKETS 10\n" +
                " REFRESH MANUAL\n" +
                " AS SELECT " +
                " time_slice(k1, interval 1 hour) AS ds, sum(v1) " +
                " FROM base_tbl1 " +
                " group by ds;");

        {
            String query = "select time_slice(k1, interval 1 hour) AS ds, sum(v1) " +
                    " FROM base_tbl1 where time_slice(k1, interval 1 hour) = '2020-02-11' group by ds";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "test_mv1", "ds = '2020-02-11 00:00:00'");
        }

        {
            String query = "select time_slice(k1, interval 1 hour) AS ds, sum(v1) " +
                    " FROM base_tbl1 where `k1` = '2020-02-11' group by ds";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "test_mv1", "ds = '2020-02-11 00:00:00'");
        }

        {
            String query = "select time_slice(k1, interval 1 hour) AS ds, sum(v1) " +
                    " FROM base_tbl1 group by ds";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "test_mv1");
        }

        executeInsertSql(connectContext, "insert into base_tbl1 partition('p3') values (\"2020-02-02\",1,1)");
        {
            String query = "select time_slice(k1, interval 1 hour) AS ds, sum(v1) " +
                    " FROM base_tbl1 group by ds";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "test_mv1", "UNION");
        }

        {
            String query = "select time_slice(k1, interval 1 hour) AS ds, sum(v1) " +
                    " FROM base_tbl1 " +
                    " WHERE time_slice(k1, interval 1 hour) >= '2020-01-01 00:00:00' " +
                    " group by ds";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "test_mv1", "UNION");
        }

        {
            String query = "select time_slice(k1, interval 1 hour) AS ds, sum(v1) " +
                    " FROM base_tbl1 " +
                    " WHERE time_slice(k1, interval 1 hour) <= '2020-03-01 00:00:00' " +
                    " group by ds";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "test_mv1", "UNION");
        }

        {
            String query = "select time_slice(k1, interval 1 hour) AS ds, sum(v1) " +
                    " FROM base_tbl1 " +
                    " WHERE time_slice(k1, interval 1 hour) >= '2020-01-01 00:00:00' and " +
                    "   time_slice(k1, interval 1 hour) <= '2020-03-01 00:00:00' " +
                    " group by ds";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "test_mv1", "UNION");
        }

        dropMv("test", "test_mv1");
        starRocksAssert.dropTable("base_tbl1");
    }

    @Test
    public void testViewBaseMvRewriteOnHive() throws Exception {
        String view = "create view hive_view_1 " +
                "as " +
                "SELECT `o_orderkey`, `o_orderstatus`, `o_orderdate`  FROM `hive0`.`partitioned_db`.`orders`";
        starRocksAssert.withView(view);
        String mv = "CREATE MATERIALIZED VIEW `view_based_mv_1`\n" +
                "PARTITION BY date_trunc('month', o_orderdate)\n" +
                "DISTRIBUTED BY HASH(`o_orderkey`) BUCKETS 10\n" +
                "REFRESH MANUAL\n" +
                "AS SELECT `o_orderkey`, `o_orderstatus`, `o_orderdate`  FROM hive_view_1";
        starRocksAssert.withMaterializedView(mv);
        refreshMaterializedView("test", "view_based_mv_1");
        {
            String query = "SELECT `o_orderkey`, `o_orderstatus`, `o_orderdate`  FROM hive_view_1";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "view_based_mv_1");
        }
        starRocksAssert.dropMaterializedView("view_based_mv_1");
    }

    @Test
    public void testViewBaseMvRewriteWithPartitionExpr() throws Exception {
        connectContext.getSessionVariable().setEnableViewBasedMvRewrite(true);
        {
            starRocksAssert.withView("create view view_with_expr" +
                    " as" +
                    " SELECT DATE_TRUNC('DAY', `id_datetime`) AS `day_date`, \n" +
                    "  COUNT(`t1a`) AS `cnt` \n" +
                    "FROM `table_with_datetime_partition` \n" +
                    "GROUP BY DATE_TRUNC('DAY', `id_datetime`)");
            createAndRefreshMv("create materialized view mv_on_view_1" +
                            " partition by day_date" +
                            " distributed by hash(`day_date`)" +
                            " refresh deferred manual" +
                            " as" +
                            " select * from view_with_expr");
            cluster.runSql("test", "insert into table_with_datetime_partition partition(p19910330)" +
                    " values(\"varchar12\", '1991-03-30', 2, 2, 1)");
            String query5 = "select * from view_with_expr";
            String plan5 = getFragmentPlan(query5);
            PlanTestBase.assertContains(plan5, "mv_on_view_1");
            PlanTestBase.assertContains(plan5, "UNION",
                    "     TABLE: table_with_datetime_partition\n" +
                            "     PREAGGREGATION: ON\n" +
                            "     partitions=1/4");
            PlanTestBase.assertContains(plan5, "     TABLE: mv_on_view_1\n" +
                    "     PREAGGREGATION: ON\n" +
                    "     partitions=3/4");
            dropMv("test", "mv_on_view_1");
            starRocksAssert.dropView("view_with_expr");
        }

        {
            starRocksAssert.withView("create view view_with_expr" +
                    " as" +
                    " SELECT DATE_TRUNC('DAY', `id_date`) AS `day_date`, \n" +
                    "  COUNT(`t1a`) AS `cnt` \n" +
                    "FROM `table_with_day_partition` \n" +
                    "GROUP BY DATE_TRUNC('DAY', `id_date`)");
            createAndRefreshMv("create materialized view mv_on_view_1" +
                            " partition by day_date" +
                            " distributed by hash(`day_date`)" +
                            " as" +
                            " select * from view_with_expr");
            cluster.runSql("test", "insert into table_with_day_partition partition(p19910330)" +
                    " values(\"varchar12\", '1991-03-30', 2, 2, 1)");
            String query5 = "select * from view_with_expr";
            String plan5 = getFragmentPlan(query5);
            PlanTestBase.assertContains(plan5, "mv_on_view_1");
            PlanTestBase.assertContains(plan5, "UNION",
                    "     TABLE: mv_on_view_1\n" +
                            "     PREAGGREGATION: ON\n" +
                            "     partitions=3/4");
            PlanTestBase.assertContains(plan5, "     TABLE: table_with_day_partition\n" +
                    "     PREAGGREGATION: ON\n" +
                    "     partitions=1/4");
            dropMv("test", "mv_on_view_1");
            starRocksAssert.dropView("view_with_expr");
        }
        connectContext.getSessionVariable().setEnableViewBasedMvRewrite(false);
    }

    @Test
    public void testMVPartitionRefreshRewrite() throws Exception {
        connectContext.getSessionVariable().setEnableMaterializedViewTransparentUnionRewrite(false);
        sql("CREATE TABLE test_base_table1(\n" +
                "    `col0`           int(11) NULL,\n" +
                "    `col2`           date NULL,\n" +
                "    `col3`           varchar(32) NULL,\n" +
                "    `id`             bigint(20) NULL,\n" +
                "    `col1`           bigint(20) NULL\n" +
                ") DUPLICATE KEY(col0, col2, col3)\n" +
                "  PARTITION BY RANGE(col2)(\n" +
                "  START (\"2022-04-01\") END (\"2022-04-10\") EVERY (INTERVAL 1 day))\n" +
                "  DISTRIBUTED BY HASH(col0)\n" +
                "  PROPERTIES(\n" +
                "    \"replication_num\" = \"1\"\n" +
                ");");
        sql("INSERT INTO test_base_table1 (col0, col2, col3) VALUES " +
                "(987654321, '2022-04-01', 'Fujian1')," +
                "(987654321, '2022-04-02', 'Fujian2')," +
                "(987654321, '2022-04-03', 'Guangdong')," +
                "(987654321, '2022-04-04', 'Fujian');");
        sql("CREATE MATERIALIZED VIEW test_mv1 \n" +
                "partition by (col2)\n" +
                "REFRESH DEFERRED MANUAL\n" +
                "AS\n" +
                "SELECT * from (select col2,col3,col0,id,col1 FROM test_base_table1 " +
                "where col3 = 'Guangdong' and col0 = 123456789) tmp;\n");

        sql("refresh materialized view test_mv1 partition start('2022-04-01') end ('2022-04-04') with sync mode;");

        String query = "select col1, col2, 'server' source_meta_type, count(1) " +
                "from test_base_table1 where col2 between '2022-04-01' and '2022-04-05'  group by col1, col2;\n";
        connectContext.getSessionVariable().setMaterializedViewUnionRewriteMode(1);
        {
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "UNION");
            // TODO: This should be rewritten since the partition range is not changed but it increased union operator,
            // TODO: so the rewritten plan's performance is not better than the original plan.
            // input query's partition range is [2022-04-01, 2022-04-05] and should not be changed.
            PlanTestBase.assertContains(plan, "     TABLE: test_base_table1\n" +
                    "     PREAGGREGATION: ON\n" +
                    "     PREDICATES: 20: col3 = 'Guangdong', 18: col0 = 123456789\n" +
                    "     partitions=2/9");
        }

        {
            sql("INSERT INTO test_base_table1 partition('p20220405') VALUES (123456789, '2022-04-05', 'Guangdong', 1, 10001)");
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "UNION");
            // input query's partition range is [2022-04-01, 2022-04-05] and should not be changed.
            PlanTestBase.assertContains(plan, "     TABLE: test_base_table1\n" +
                    "     PREAGGREGATION: ON\n" +
                    "     PREDICATES: ((23: col0 != 123456789) OR ((23: col0 = 123456789) " +
                    "AND (25: col3 = 'Guangdong') IS NULL)) OR (25: col3 != 'Guangdong')\n" +
                    "     partitions=5/9");
        }
        connectContext.getSessionVariable().setMaterializedViewUnionRewriteMode(0);
        connectContext.getSessionVariable().setEnableMaterializedViewTransparentUnionRewrite(true);
        starRocksAssert.dropTable("test_base_table1");
        starRocksAssert.dropMaterializedView("test_mv1");
    }
}
