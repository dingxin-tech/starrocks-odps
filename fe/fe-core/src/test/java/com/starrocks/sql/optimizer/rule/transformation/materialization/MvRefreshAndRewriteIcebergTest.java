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

import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvPlanContext;
import com.starrocks.catalog.Partition;
import com.starrocks.common.Config;
import com.starrocks.connector.iceberg.MockIcebergMetadata;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.MaterializedViewAnalyzer;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.optimizer.CachingMvPlanContextBuilder;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer.MethodName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@TestMethodOrder(MethodName.class)
public class MvRefreshAndRewriteIcebergTest extends MVTestBase {
    @BeforeAll
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();
        ConnectorPlanTestBase.mockCatalog(connectContext, MockIcebergMetadata.MOCKED_ICEBERG_CATALOG_NAME);
        connectContext.getSessionVariable().setMaterializedViewUnionRewriteMode(1);
        connectContext.getSessionVariable().setEnableMaterializedViewTransparentUnionRewrite(false);
    }

    @Test
    public void testStr2DateMVRefreshRewriteSingleTable_UnionRewrite() throws Exception {
        String mvName = "test_mv1";
        starRocksAssert.withMaterializedView("create materialized view " + mvName + " " +
                    "partition by str2date(d,'%Y-%m-%d') " +
                    "distributed by hash(a) " +
                    "REFRESH DEFERRED MANUAL " +
                    "PROPERTIES (\n" +
                    "'replication_num' = '1'" +
                    ") " +
                    "as select a, b, d, bitmap_union(to_bitmap(t1.c))" +
                    " from iceberg0.partitioned_db.part_tbl1 as t1 " +
                    " group by a, b, d;");

        testSingleTableWithMVRewrite(mvName);

        starRocksAssert.dropMaterializedView(mvName);
        connectContext.getSessionVariable().setMaterializedViewRewriteMode("default");
    }

    @Test
    public void testStr2DateMVRefreshRewriteSingleTableWithView() throws Exception {
        String mvName = "test_mv1";
        starRocksAssert.withView("CREATE VIEW view1 as select a, b, d, bitmap_union(to_bitmap(t1.c))\n" +
                    " from iceberg0.partitioned_db.part_tbl1 as t1 \n" +
                    " group by a, b, d;");
        starRocksAssert.withMaterializedView("create materialized view " + mvName + " " +
                    "partition by str2date(d,'%Y-%m-%d') " +
                    "distributed by hash(a) " +
                    "REFRESH DEFERRED MANUAL " +
                    "PROPERTIES (\n" +
                    "'replication_num' = '1'" +
                    ") " +
                    "as select * from view1");

        testSingleTableWithMVRewrite(mvName);

        starRocksAssert.dropMaterializedView(mvName);
        starRocksAssert.dropView("view1");
        connectContext.getSessionVariable().setMaterializedViewRewriteMode("default");
    }

    private void testSingleTableWithMVRewrite(String mvName) throws Exception {
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView materializedView = ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(testDb.getFullName(), mvName));

        // initial create
        starRocksAssert.getCtx().executeSql("refresh materialized view " + mvName + " partition start('2023-08-01') " +
                    "end ('2023-08-02') force with sync mode");
        List<String> partitions =
                    materializedView.getPartitions().stream().map(Partition::getName).sorted()
                                .collect(Collectors.toList());
        Assertions.assertEquals(Arrays.asList("p20230801_20230802"), partitions);

        {
            String query = "select t1.a, t2.b, t1.d, count(distinct t1.c)\n" +
                        " from  iceberg0.partitioned_db.part_tbl1 as t1 \n" +
                        " left join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d \n" +
                        " left join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d \n" +
                        " where t1.d in ('2023-08-01')\n" +
                        " group by t1.a, t2.b, t1.d;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "test_mv1");
        }

        connectContext.getSessionVariable().setMaterializedViewRewriteMode("force");
        {
            String query = "select a, b, d, count(distinct t1.c)\n" +
                        " from iceberg0.partitioned_db.part_tbl1 as t1 \n" +
                        " where d='2023-08-01'" +
                        " group by a, b, d;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertNotContains(plan, "UNION");
            PlanTestBase.assertContains(plan, "0:OlapScanNode\n" +
                        "     TABLE: test_mv1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     partitions=1/1");
        }

        {
            String query = "select a, b, d, count(distinct t1.c)\n" +
                        " from iceberg0.partitioned_db.part_tbl1 as t1 \n" +
                        " where t1.d >= '2023-08-01' \n" +
                        " group by a, b, d;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "UNION");

            PlanTestBase.assertContains(plan, "OlapScanNode\n" +
                        "     TABLE: test_mv1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     partitions=1/1");
            PlanTestBase.assertContains(plan, "     TABLE: partitioned_db.part_tbl1\n" +
                    "     PREDICATES: 21: d >= '2023-08-01', 21: d IN ('2023-08-03', '2023-08-02')");
        }
        {
            String query = "select a, b, d, count(distinct t1.c)\n" +
                        " from iceberg0.partitioned_db.part_tbl1 as t1 \n" +
                        " group by a, b, d;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "UNION");

            PlanTestBase.assertContains(plan, "OlapScanNode\n" +
                        "     TABLE: test_mv1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     partitions=1/1");
            PlanTestBase.assertContains(plan, "TABLE: partitioned_db.part_tbl1\n" +
                    "     PREDICATES: 21: d IN ('2023-08-03', '2023-08-02')");
        }

        starRocksAssert.getCtx().executeSql("refresh materialized view " + mvName + " partition start('2023-08-02') " +
                    "end ('2023-08-03') force with sync mode");
        partitions =
                    materializedView.getPartitions().stream().map(Partition::getName).sorted()
                                .collect(Collectors.toList());
        Assertions.assertEquals(Arrays.asList("p20230801_20230802", "p20230802_20230803"), partitions);

        {
            String query = "select a, b, d, count(distinct t1.c)\n" +
                        " from iceberg0.partitioned_db.part_tbl1 as t1 \n" +
                        " where t1.d >= '2023-08-01' \n" +
                        " group by a, b, d;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "UNION");

            PlanTestBase.assertContains(plan, "OlapScanNode\n" +
                        "     TABLE: test_mv1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     partitions=2/2");
            PlanTestBase.assertContains(plan, "TABLE: partitioned_db.part_tbl1\n" +
                    "     PREDICATES: 21: d >= '2023-08-01', 21: d = '2023-08-03'");
        }
        {
            String query = "select a, b, d, count(distinct t1.c)\n" +
                        " from iceberg0.partitioned_db.part_tbl1 as t1 \n" +
                        " group by a, b, d;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "UNION");

            PlanTestBase.assertContains(plan, "OlapScanNode\n" +
                        "     TABLE: test_mv1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     partitions=2/2");
            PlanTestBase.assertContains(plan, "TABLE: partitioned_db.part_tbl1\n" +
                    "     PREDICATES: 21: d IN ('2023-08-03')");
        }
    }

    @Test
    public void testStr2DateMVRefreshRewrite_InnerJoin_FullRefresh() throws Exception {
        String mvName = "test_mv1";
        starRocksAssert.withMaterializedView("create materialized view " + mvName + " " +
                    "partition by str2date(d,'%Y-%m-%d') " +
                    "distributed by hash(a) " +
                    "REFRESH DEFERRED MANUAL " +
                    "PROPERTIES (\n" +
                    "'replication_num' = '1'" +
                    ") " +
                    "as select  t1.a, t2.b, t3.c, t1.d " +
                    " from  iceberg0.partitioned_db.part_tbl1 as t1 " +
                    " inner join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d " +
                    " inner join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d ;");
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView materializedView = ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(testDb.getFullName(), mvName));

        // initial create
        starRocksAssert.getCtx().executeSql("refresh materialized view " + mvName + " force with sync mode");
        List<String> partitions =
                    materializedView.getPartitions().stream().map(Partition::getName).sorted()
                                .collect(Collectors.toList());
        Assertions.assertEquals(Arrays.asList("p20230801_20230802", "p20230802_20230803", "p20230803_20230804"), partitions);

        {
            String query = "select  t1.a, t2.b, t3.c, t1.d " +
                        " from  iceberg0.partitioned_db.part_tbl1 as t1 " +
                        " inner join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d " +
                        " inner join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d " +
                        " where t1.d='2023-08-01';";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "0:OlapScanNode\n" +
                        "     TABLE: test_mv1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     partitions=1/3\n" +
                        "     rollup: test_mv1");
        }

        {
            String query = "select  t1.a, t2.b, t3.c, t1.d " +
                        " from  iceberg0.partitioned_db.part_tbl1 as t1 " +
                        " inner join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d " +
                        " inner join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d " +
                        " where t1.d>='20230801';";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "0:OlapScanNode\n" +
                        "     TABLE: test_mv1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 16: d >= '20230801'\n" +
                        "     partitions=3/3\n" +
                        "     rollup: test_mv1");
        }

        {
            String query = "select  t1.a, t2.b, t3.c, t1.d " +
                        " from  iceberg0.partitioned_db.part_tbl1 as t1 " +
                        " inner join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d " +
                        " inner join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d " +
                        " where t1.d < '20230802';";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "0:OlapScanNode\n" +
                        "     TABLE: test_mv1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     partitions=1/3\n" +
                        "     rollup: test_mv1");
        }

        starRocksAssert.dropMaterializedView(mvName);
    }

    @Test
    public void testStr2DateMVRefreshRewrite_InnerJoin_PartialRefresh() throws Exception {
        String mvName = "test_mv1";
        starRocksAssert.withMaterializedView("create materialized view " + mvName + " " +
                    "partition by str2date(d,'%Y-%m-%d') " +
                    "distributed by hash(a) " +
                    "REFRESH DEFERRED MANUAL " +
                    "PROPERTIES (\n" +
                    "'replication_num' = '1'" +
                    ") " +
                    "as select  t1.a, t2.b, t3.c, t1.d " +
                    " from  iceberg0.partitioned_db.part_tbl1 as t1 " +
                    " inner join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d " +
                    " inner join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d ;");
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView materializedView = ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(testDb.getFullName(), mvName));

        // initial create
        starRocksAssert.getCtx().executeSql("refresh materialized view " + mvName + " partition start('2023-08-01') " +
                    "end ('2023-08-02') force with sync mode");
        List<String> partitions =
                    materializedView.getPartitions().stream().map(Partition::getName).sorted()
                                .collect(Collectors.toList());
        Assertions.assertEquals(Arrays.asList("p20230801_20230802"), partitions);

        {
            String query = "select  t1.a, t2.b, t3.c, t1.d " +
                        " from  iceberg0.partitioned_db.part_tbl1 as t1 " +
                        " inner join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d " +
                        " inner join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d " +
                        " where t1.d='2023-08-01';";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "0:OlapScanNode\n" +
                        "     TABLE: test_mv1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     partitions=1/1\n" +
                        "     rollup: test_mv1");
        }

        {
            String query = "select  t1.a, t2.b, t3.c, t1.d " +
                        " from  iceberg0.partitioned_db.part_tbl1 as t1 " +
                        " inner join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d " +
                        " inner join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d " +
                        " where t1.d>='20230801';";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "test_mv1");
        }

        {
            String query = "select  t1.a, t2.b, t3.c, t1.d " +
                        " from  iceberg0.partitioned_db.part_tbl1 as t1 " +
                        " inner join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d " +
                        " inner join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d " +
                        " where t1.d < '20230802';";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "UNION");
            PlanTestBase.assertContains(plan, "12:OlapScanNode\n" +
                        "     TABLE: test_mv1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     partitions=1/1\n" +
                        "     rollup: test_mv1");
        }

        starRocksAssert.dropMaterializedView(mvName);
    }

    @Test
    public void testStr2DateMVRefreshRewrite_LeftJoin_FullRefresh() throws Exception {
        String mvName = "test_mv1";
        starRocksAssert.withMaterializedView("create materialized view " + mvName + " " +
                    "partition by str2date(d,'%Y-%m-%d') " +
                    "distributed by hash(a) " +
                    "REFRESH DEFERRED MANUAL " +
                    "PROPERTIES (\n" +
                    "'replication_num' = '1'" +
                    ") " +
                    "as select  t1.a, t2.b, t3.c, t1.d " +
                    " from  iceberg0.partitioned_db.part_tbl1 as t1 " +
                    " left join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d " +
                    " left join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d ;");
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView materializedView = ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(testDb.getFullName(), mvName));

        // initial create
        starRocksAssert.getCtx().executeSql("refresh materialized view " + mvName + " force with sync mode");
        List<String> partitions =
                    materializedView.getPartitions().stream().map(Partition::getName).sorted()
                                .collect(Collectors.toList());
        Assertions.assertEquals(Arrays.asList("p20230801_20230802", "p20230802_20230803", "p20230803_20230804"), partitions);

        {
            String query = "select  t1.a, t2.b, t3.c, t1.d " +
                        " from  iceberg0.partitioned_db.part_tbl1 as t1 " +
                        " left join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d " +
                        " left join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d " +
                        " where t1.d='2023-08-01';";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "0:OlapScanNode\n" +
                        "     TABLE: test_mv1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     partitions=1/3\n" +
                        "     rollup: test_mv1");
        }

        {
            String query = "select  t1.a, t2.b, t3.c, t1.d " +
                        " from  iceberg0.partitioned_db.part_tbl1 as t1 " +
                        " left join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d " +
                        " left join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d " +
                        " where t1.d>='20230801';";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "0:OlapScanNode\n" +
                        "     TABLE: test_mv1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 16: d >= '20230801'\n" +
                        "     partitions=3/3\n" +
                        "     rollup: test_mv1");
        }

        {
            String query = "select  t1.a, t2.b, t3.c, t1.d " +
                        " from  iceberg0.partitioned_db.part_tbl1 as t1 " +
                        " left join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d " +
                        " left join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d " +
                        " where t1.d < '20230802';";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "0:OlapScanNode\n" +
                        "     TABLE: test_mv1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     partitions=1/3\n" +
                        "     rollup: test_mv1");
        }

        starRocksAssert.dropMaterializedView(mvName);
    }

    @Test
    public void testStr2DateMVRefreshRewrite_LeftJoin_PartialRefresh() throws Exception {
        String mvName = "test_mv1";
        starRocksAssert.withMaterializedView("create materialized view " + mvName + " " +
                    "partition by str2date(d,'%Y-%m-%d') " +
                    "distributed by hash(a) " +
                    "REFRESH DEFERRED MANUAL " +
                    "PROPERTIES (\n" +
                    "'replication_num' = '1'" +
                    ") " +
                    "as select  t1.a, t2.b, t3.c, t1.d " +
                    " from  iceberg0.partitioned_db.part_tbl1 as t1 " +
                    " left join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d " +
                    " left join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d ;");
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView materializedView = ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(testDb.getFullName(), mvName));

        // initial create
        starRocksAssert.getCtx().executeSql("refresh materialized view " + mvName + " partition start('2023-08-01') " +
                    "end ('2023-08-02') force with sync mode");
        List<String> partitions =
                    materializedView.getPartitions().stream().map(Partition::getName).sorted()
                                .collect(Collectors.toList());
        Assertions.assertEquals(Arrays.asList("p20230801_20230802"), partitions);

        {
            String query = "select  t1.a, t2.b, t3.c, t1.d " +
                        " from  iceberg0.partitioned_db.part_tbl1 as t1 " +
                        " left join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d " +
                        " left join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d " +
                        " where t1.d='2023-08-01';";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "0:OlapScanNode\n" +
                        "     TABLE: test_mv1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     partitions=1/1\n" +
                        "     rollup: test_mv1");
        }

        {
            String query = "select  t1.a, t2.b, t3.c, t1.d " +
                        " from  iceberg0.partitioned_db.part_tbl1 as t1 " +
                        " left join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d " +
                        " left join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d " +
                        " where t1.d>='20230801';";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "test_mv1");
        }

        {
            String query = "select  t1.a, t2.b, t3.c, t1.d " +
                        " from  iceberg0.partitioned_db.part_tbl1 as t1 " +
                        " left join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d " +
                        " left join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d " +
                        " where t1.d < '20230802';";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "UNION");
            PlanTestBase.assertContains(plan, "12:OlapScanNode\n" +
                        "     TABLE: test_mv1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     partitions=1/1\n" +
                        "     rollup: test_mv1");
        }

        {
            String query = "select  t1.a, t2.b, t3.c, t1.d " +
                        " from  iceberg0.partitioned_db.part_tbl1 as t1 " +
                        " left join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d " +
                        " left join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d " +
                        " where t1.d>='2023-08-01';";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "test_mv1");
        }

        {
            String query = "select  t1.a, t2.b, t3.c, t1.d " +
                        " from  iceberg0.partitioned_db.part_tbl1 as t1 " +
                        " left join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d " +
                        " left join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d " +
                        " where t1.d < '2023-08-02';";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "UNION");
            PlanTestBase.assertContains(plan, "12:OlapScanNode\n" +
                        "     TABLE: test_mv1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     partitions=1/1\n" +
                        "     rollup: test_mv1");
        }
        starRocksAssert.dropMaterializedView(mvName);
    }

    @Test
    public void testStr2DateMVRefreshRewrite_InnerJoin_PartitionPrune1() throws Exception {
        String mvName = "test_mv1";
        starRocksAssert.withMaterializedView("create materialized view " + mvName + " " +
                    "partition by str2date(d,'%Y-%m-%d') " +
                    "distributed by hash(a) " +
                    "REFRESH DEFERRED MANUAL " +
                    "PROPERTIES (\n" +
                    "'replication_num' = '1'" +
                    ") " +
                    "as select  t1.a, t2.b, t3.c, t1.d " +
                    " from  iceberg0.partitioned_db.part_tbl1 as t1 " +
                    " inner join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d " +
                    " inner join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d ;");
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView materializedView = ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(testDb.getFullName(), mvName));

        // initial create
        starRocksAssert.getCtx().executeSql("refresh materialized view " + mvName + " force with sync mode");
        List<String> partitions =
                    materializedView.getPartitions().stream().map(Partition::getName).sorted()
                                .collect(Collectors.toList());
        Assertions.assertEquals(Arrays.asList("p20230801_20230802", "p20230802_20230803", "p20230803_20230804"), partitions);

        {
            String query = "select  count(*) from " + mvName +
                        " where d='2023-08-01';";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "0:OlapScanNode\n" +
                        "     TABLE: test_mv1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     partitions=1/3\n" +
                        "     rollup: test_mv1");
        }

        {
            String query = "select  count(*) from " + mvName +
                        " where d in ('2023-08-01');";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "0:OlapScanNode\n" +
                        "     TABLE: test_mv1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     partitions=1/3");
        }

        {
            String query = "select  count(*) from " + mvName +
                        " where d in ('2023-08-01', '2023-08-02');";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "0:OlapScanNode\n" +
                        "     TABLE: test_mv1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 4: d IN ('2023-08-01', '2023-08-02')\n" +
                        "     partitions=2/3\n" +
                        "     rollup: test_mv1");
        }

        {
            String query = "select  count(*) from " + mvName +
                        " where d in ('2023-08-01', '2023-08-02', '2023-08-03');";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "0:OlapScanNode\n" +
                        "     TABLE: test_mv1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 4: d IN ('2023-08-01', '2023-08-02', '2023-08-03')\n" +
                        "     partitions=3/3");
        }

        {
            String query = "select  count(*) from " + mvName +
                        " where d not in ('2023-08-01', '2023-08-02');";
            String plan = getFragmentPlan(query);
            // TODO: no partition prune
            PlanTestBase.assertContains(plan, "0:OlapScanNode\n" +
                        "     TABLE: test_mv1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 4: d NOT IN ('2023-08-01', '2023-08-02')\n" +
                        "     partitions=3/3\n" +
                        "     rollup: test_mv1");
        }

        {
            String query = "select  count(*) from " + mvName +
                        " where d >= '2023-08-01' and d < '2023-08-02';";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "0:OlapScanNode\n" +
                        "     TABLE: test_mv1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     partitions=1/3\n" +
                        "     rollup: test_mv1");
        }

        {
            String query = "select  count(*) from " + mvName +
                        " where cast(d as date) >= '2023-08-01'";
            String plan = getFragmentPlan(query);
            // TODO: no partition prune
            PlanTestBase.assertContains(plan, "0:OlapScanNode\n" +
                        "     TABLE: test_mv1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: CAST(4: d AS DATE) >= '2023-08-01'\n" +
                        "     partitions=3/3");
        }

        connectContext.getSessionVariable().setRangePrunerPredicateMaxLen(0);
        {
            String query = "select  count(*) from " + mvName +
                        " where d in ('2023-08-01');";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "0:OlapScanNode\n" +
                        "     TABLE: test_mv1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     partitions=1/3");
        }

        {
            String query = "select  count(*) from " + mvName +
                        " where d in ('2023-08-01', '2023-08-02');";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "0:OlapScanNode\n" +
                        "     TABLE: test_mv1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 4: d IN ('2023-08-01', '2023-08-02')\n" +
                        "     partitions=2/3\n" +
                        "     rollup: test_mv1");
        }

        {
            String query = "select  count(*) from " + mvName +
                        " where d in ('2023-08-01', '2023-08-02', '2023-08-03');";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "0:OlapScanNode\n" +
                        "     TABLE: test_mv1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 4: d IN ('2023-08-01', '2023-08-02', '2023-08-03')\n" +
                        "     partitions=3/3");
        }

        starRocksAssert.dropMaterializedView(mvName);
    }

    @Test
    public void testStr2DateMVRefreshRewrite_InnerJoin_PartitionPrune2() throws Exception {
        String mvName = "test_mv1";
        starRocksAssert.withMaterializedView("create materialized view " + mvName + " " +
                    "partition by str2date(d,'%Y-%m-%d') " +
                    "distributed by hash(a) " +
                    "REFRESH DEFERRED MANUAL " +
                    "PROPERTIES (\n" +
                    "'replication_num' = '1'" +
                    ") " +
                    "as select  t1.a, t2.b, t3.c, t1.d " +
                    " from  iceberg0.partitioned_db.part_tbl1 as t1 " +
                    " inner join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d " +
                    " inner join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d ;");
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView materializedView = ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(testDb.getFullName(), mvName));

        // initial create
        starRocksAssert.getCtx().executeSql("refresh materialized view " + mvName + " force with sync mode");
        List<String> partitions =
                    materializedView.getPartitions().stream().map(Partition::getName).sorted()
                                .collect(Collectors.toList());
        Assertions.assertEquals(Arrays.asList("p20230801_20230802", "p20230802_20230803", "p20230803_20230804"), partitions);

        {
            String query = "select  t1.a, t2.b, t3.c, t1.d \n" +
                        " from  iceberg0.partitioned_db.part_tbl1 as t1 \n" +
                        " inner join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d \n" +
                        " inner join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d \n" +
                        " where t1.d='2023-08-01';";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "0:OlapScanNode\n" +
                        "     TABLE: test_mv1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     partitions=1/3\n" +
                        "     rollup: test_mv1");
        }

        {
            String query = "select  t1.a, t2.b, t3.c, t1.d \n" +
                        " from  iceberg0.partitioned_db.part_tbl1 as t1 \n" +
                        " inner join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d \n" +
                        " inner join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d \n" +
                        " where t1.d in ('2023-08-01');";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "0:OlapScanNode\n" +
                        "     TABLE: test_mv1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     partitions=1/3");
        }

        {
            String query = "select  t1.a, t2.b, t3.c, t1.d \n" +
                        " from  iceberg0.partitioned_db.part_tbl1 as t1 \n" +
                        " inner join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d \n" +
                        " inner join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d \n" +
                        " where t1.d in ('2023-08-01', '2023-08-02');";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "0:OlapScanNode\n" +
                        "     TABLE: test_mv1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 16: d IN ('2023-08-01', '2023-08-02')\n" +
                        "     partitions=2/3");
        }

        {
            String query = "select  t1.a, t2.b, t3.c, t1.d \n" +
                        " from  iceberg0.partitioned_db.part_tbl1 as t1 \n" +
                        " inner join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d \n" +
                        " inner join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d \n" +
                        " where t1.d in ('2023-08-01', '2023-08-02', '2023-08-03');";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "0:OlapScanNode\n" +
                        "     TABLE: test_mv1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 16: d IN ('2023-08-01', '2023-08-02', '2023-08-03')\n" +
                        "     partitions=3/3");
        }

        {
            String query = "select  t1.a, t2.b, t3.c, t1.d \n" +
                        " from  iceberg0.partitioned_db.part_tbl1 as t1 \n" +
                        " inner join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d \n" +
                        " inner join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d \n" +
                        " where t1.d not in ('2023-08-01', '2023-08-02');";
            String plan = getFragmentPlan(query);
            // TODO: no partition prune
            PlanTestBase.assertContains(plan, "0:OlapScanNode\n" +
                        "     TABLE: test_mv1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 16: d != '2023-08-01', 16: d != '2023-08-02'\n" +
                        "     partitions=3/3");
        }

        {
            String query = "select  t1.a, t2.b, t3.c, t1.d \n" +
                        " from  iceberg0.partitioned_db.part_tbl1 as t1 \n" +
                        " inner join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d \n" +
                        " inner join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d \n" +
                        " where t1.d >= '2023-08-01' and t1.d < '2023-08-02';";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "0:OlapScanNode\n" +
                        "     TABLE: test_mv1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     partitions=1/3\n" +
                        "     rollup: test_mv1");
        }

        {
            String query = "select  t1.a, t2.b, t3.c, t1.d \n" +
                        " from  iceberg0.partitioned_db.part_tbl1 as t1 \n" +
                        " inner join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d \n" +
                        " inner join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d \n" +
                        " where cast(t1.d as date) >= '2023-08-01'";
            String plan = getFragmentPlan(query);
            // TODO: no partition prune
            PlanTestBase.assertContains(plan, "0:OlapScanNode\n" +
                        "     TABLE: test_mv1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: CAST(16: d AS DATE) >= '2023-08-01'\n" +
                        "     partitions=3/3");
        }

        connectContext.getSessionVariable().setRangePrunerPredicateMaxLen(0);
        {
            String query = "select  t1.a, t2.b, t3.c, t1.d \n" +
                        " from  iceberg0.partitioned_db.part_tbl1 as t1 \n" +
                        " inner join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d \n" +
                        " inner join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d \n" +
                        " where t1.d in ('2023-08-01');";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "0:OlapScanNode\n" +
                        "     TABLE: test_mv1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     partitions=1/3");
        }

        {
            String query = "select  t1.a, t2.b, t3.c, t1.d \n" +
                        " from  iceberg0.partitioned_db.part_tbl1 as t1 \n" +
                        " inner join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d \n" +
                        " inner join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d \n" +
                        " where t1.d in ('2023-08-01', '2023-08-02');";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "0:OlapScanNode\n" +
                        "     TABLE: test_mv1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 16: d IN ('2023-08-01', '2023-08-02')\n" +
                        "     partitions=2/3");
        }

        {
            String query = "select  t1.a, t2.b, t3.c, t1.d \n" +
                        " from  iceberg0.partitioned_db.part_tbl1 as t1 \n" +
                        " inner join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d \n" +
                        " inner join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d \n" +
                        " where t1.d in ('2023-08-01', '2023-08-02', '2023-08-03');";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "0:OlapScanNode\n" +
                        "     TABLE: test_mv1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 16: d IN ('2023-08-01', '2023-08-02', '2023-08-03')\n" +
                        "     partitions=3/3");
        }

        starRocksAssert.dropMaterializedView(mvName);
    }

    @Test
    public void testStr2DateMVRefreshRewrite_InnerJoin_ExtraCompensate() throws Exception {
        String mvName = "test_mv1";
        starRocksAssert.withMaterializedView("create materialized view " + mvName + " " +
                    "partition by str2date(d,'%Y-%m-%d') " +
                    "distributed by hash(b) " +
                    "REFRESH DEFERRED MANUAL " +
                    "PROPERTIES (\n" +
                    "'replication_num' = '1'" +
                    ") " +
                    "as select  t1.d, t2.b, t3.c, count(t1.a) " +
                    " from  iceberg0.partitioned_db.part_tbl1 as t1 " +
                    " inner join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d " +
                    " inner join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d " +
                    " group by t1.d, t2.b, t3.c;");
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView materializedView = ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(testDb.getFullName(), mvName));

        // initial create
        starRocksAssert.getCtx().executeSql("refresh materialized view " + mvName + " partition start('2023-08-01') " +
                    "end ('2023-08-02') force with sync mode");
        List<String> partitions =
                    materializedView.getPartitions().stream().map(Partition::getName).sorted()
                                .collect(Collectors.toList());
        Assertions.assertEquals(Arrays.asList("p20230801_20230802"), partitions);

        {
            String query = "select  t1.d, t2.b, t3.c, count(t1.a) " +
                        " from  iceberg0.partitioned_db.part_tbl1 as t1 " +
                        " inner join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d " +
                        " inner join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d " +
                        " where t1.d='2023-08-01' " +
                        " group by t1.d, t2.b, t3.c;";

            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "0:OlapScanNode\n" +
                        "     TABLE: test_mv1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     partitions=1/1");
        }
        connectContext.getSessionVariable().setMaterializedViewRewriteMode("force");
        {
            String query = "select  t1.d, t2.b, t3.c, count(t1.a) " +
                        " from  iceberg0.partitioned_db.part_tbl1 as t1 " +
                        " inner join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d " +
                        " inner join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d " +
                        " where t1.d>='2023-08-01' " +
                        " group by t1.d, t2.b, t3.c;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "UNION");
            PlanTestBase.assertContains(plan, "13:OlapScanNode\n" +
                        "     TABLE: test_mv1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     partitions=1/1\n" +
                        "     rollup: test_mv1");
        }
        {
            String query = "select  t1.d, t2.b, t3.c, count(t1.a) " +
                        " from  iceberg0.partitioned_db.part_tbl1 as t1 " +
                        " inner join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d " +
                        " inner join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d " +
                        " where t1.d>='2023-08-01' and t2.b != '' and t2.b is not null " +
                        " group by t1.d, t2.b, t3.c;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "UNION");
            assertContainsMV(plan);
        }

        {
            String query = "select  t1.d, t2.b, t3.c, count(t1.a) " +
                        " from  iceberg0.partitioned_db.part_tbl1 as t1 " +
                        " inner join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d " +
                        " inner join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d " +
                        " where t1.d>='2023-08-01' and t2.b != '' and t2.b is not null and t2.b in ('xxx') " +
                        " and t3.c in ('xxx') " +
                        " group by t1.d, t2.b, t3.c;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "UNION");
            assertContainsMV(plan);
        }
        starRocksAssert.dropMaterializedView(mvName);
        connectContext.getSessionVariable().setMaterializedViewRewriteMode("default");
    }

    @Test
    public void testStr2DateMVRefreshRewrite_LeftJoin_OnPredicates_ExtraCompensate() throws Exception {
        String mvName = "test_mv1";
        starRocksAssert.withMaterializedView("create materialized view " + mvName + " " +
                    "partition by str2date(d,'%Y-%m-%d') " +
                    "distributed by hash(b) " +
                    "REFRESH DEFERRED MANUAL " +
                    "PROPERTIES (\n" +
                    "'replication_num' = '1'" +
                    ") " +
                    "as select  t1.d, t2.b, t3.c, count(t1.a) " +
                    " from  iceberg0.partitioned_db.part_tbl1 as t1 " +
                    " left join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d " +
                    " left join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d " +
                    " group by t1.d, t2.b, t3.c;");
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView materializedView = ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(testDb.getFullName(), mvName));

        // initial create
        starRocksAssert.getCtx().executeSql("refresh materialized view " + mvName + " partition start('2023-08-01') " +
                    "end ('2023-08-02') force with sync mode");
        List<String> partitions =
                    materializedView.getPartitions().stream().map(Partition::getName).sorted()
                                .collect(Collectors.toList());
        Assertions.assertEquals(Arrays.asList("p20230801_20230802"), partitions);

        {
            String query = "select  t1.d, t2.b, t3.c, count(t1.a) " +
                        " from  iceberg0.partitioned_db.part_tbl1 as t1 " +
                        " left join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d and t2.b is not null " +
                        " left join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d " +
                        " where t1.d='2023-08-01' " +
                        " group by t1.d, t2.b, t3.c;";

            String plan = getFragmentPlan(query);
            PlanTestBase.assertNotContains(plan, "UNION");
            PlanTestBase.assertContains(plan, "0:OlapScanNode\n" +
                        "     TABLE: test_mv1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     partitions=1/1");
        }
        {
            String query = "select  t1.d, t2.b, t3.c, count(t1.a) " +
                        " from  iceberg0.partitioned_db.part_tbl1 as t1 " +
                        " left join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d and t2.b != '' " +
                        " left join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d " +
                        " where t1.d='2023-08-01' " +
                        " group by t1.d, t2.b, t3.c;";

            String plan = getFragmentPlan(query);
            PlanTestBase.assertNotContains(plan, "UNION");
            PlanTestBase.assertNotContains(plan, "test_mv1");
        }
        {
            String query = "select  t1.d, t2.b, t3.c, count(t1.a) " +
                        " from  iceberg0.partitioned_db.part_tbl1 as t1 " +
                        " left join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d and t2.b != '' " +
                        " left join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d " +
                        " where t1.d>='2023-08-01' " +
                        " group by t1.d, t2.b, t3.c;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertNotContains(plan, "UNION");
            PlanTestBase.assertNotContains(plan, "test_mv1");
        }

        {
            String query = "select  t1.d, t2.b, t3.c, count(t1.a) " +
                        " from  iceberg0.partitioned_db.part_tbl1 as t1 " +
                        " left join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d and t2.b != '' " +
                        " left join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d " +
                        " where t1.d in ('2023-08-01', '2023-08-02') " +
                        " group by t1.d, t2.b, t3.c;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertNotContains(plan, "UNION");
            PlanTestBase.assertNotContains(plan, "test_mv1");
        }

        starRocksAssert.getCtx().executeSql("refresh materialized view " + mvName + " partition start('2023-08-02') " +
                    "end ('2023-08-03') force with sync mode");
        partitions =
                    materializedView.getPartitions().stream().map(Partition::getName).sorted()
                                .collect(Collectors.toList());
        Assertions.assertEquals(Arrays.asList("p20230801_20230802", "p20230802_20230803"), partitions);

        {
            String query = "select  t1.d, t2.b, t3.c, count(t1.a) " +
                        " from  iceberg0.partitioned_db.part_tbl1 as t1 " +
                        " left join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d and t2.b is not null " +
                        " left join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d " +
                        " where t1.d>='2023-08-01' " +
                        " group by t1.d, t2.b, t3.c;";

            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "UNION");
            PlanTestBase.assertContains(plan, "3:IcebergScanNode\n" +
                        "     TABLE: partitioned_db.part_tbl1\n" +
                        "     PREDICATES: 19: d >= '2023-08-01'");
        }

        {
            String query = "select  t1.d, t2.b, t3.c, count(t1.a) " +
                        " from  iceberg0.partitioned_db.part_tbl1 as t1 " +
                        " left join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d and t2.b is not null " +
                        " left join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d " +
                        " where t1.d in ('2023-08-01', '2023-08-02') " +
                        " group by t1.d, t2.b, t3.c;";

            String plan = getFragmentPlan(query);
            PlanTestBase.assertNotContains(plan, "UNION");
            PlanTestBase.assertContains(plan, "test_mv1");
        }
        starRocksAssert.dropMaterializedView(mvName);
    }

    @Test
    public void testStr2DateMVRefreshRewrite_InnerJoin_OnPredicates_ExtraCompensate() throws Exception {
        String mvName = "test_mv1";
        starRocksAssert.withMaterializedView("create materialized view " + mvName + " " +
                    "partition by str2date(d,'%Y-%m-%d') " +
                    "distributed by hash(b) " +
                    "REFRESH DEFERRED MANUAL " +
                    "PROPERTIES (\n" +
                    "'replication_num' = '1'" +
                    ") " +
                    "as select  t1.d, t2.b, t3.c, count(t1.a) " +
                    " from  iceberg0.partitioned_db.part_tbl1 as t1 " +
                    " inner join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d " +
                    " inner join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d " +
                    " group by t1.d, t2.b, t3.c;");
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView materializedView = ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(testDb.getFullName(), mvName));

        // initial create
        starRocksAssert.getCtx().executeSql("refresh materialized view " + mvName + " partition start('2023-08-01') " +
                    "end ('2023-08-02') force with sync mode");
        List<String> partitions =
                    materializedView.getPartitions().stream().map(Partition::getName).sorted()
                                .collect(Collectors.toList());
        Assertions.assertEquals(Arrays.asList("p20230801_20230802"), partitions);

        {
            String query = "select  t1.d, t2.b, t3.c, count(t1.a) " +
                        " from  iceberg0.partitioned_db.part_tbl1 as t1 " +
                        " inner join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d and t2.b is not null " +
                        " inner join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d " +
                        " where t1.d='2023-08-01' " +
                        " group by t1.d, t2.b, t3.c;";

            String plan = getFragmentPlan(query);
            PlanTestBase.assertNotContains(plan, "UNION");
            PlanTestBase.assertContains(plan, "0:OlapScanNode\n" +
                        "     TABLE: test_mv1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     partitions=1/1");
        }
        {
            String query = "select  t1.d, t2.b, t3.c, count(t1.a) " +
                        " from  iceberg0.partitioned_db.part_tbl1 as t1 " +
                        " inner join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d and t2.b != '' " +
                        " inner join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d " +
                        " where t1.d='2023-08-01' " +
                        " group by t1.d, t2.b, t3.c;";

            String plan = getFragmentPlan(query);
            PlanTestBase.assertNotContains(plan, "UNION");
            PlanTestBase.assertContains(plan, "test_mv1");
        }
        {
            String query = "select  t1.d, t2.b, t3.c, count(t1.a) " +
                        " from  iceberg0.partitioned_db.part_tbl1 as t1 " +
                        " inner join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d and t2.b != '' " +
                        " inner join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d " +
                        " where t1.d>='2023-08-01' " +
                        " group by t1.d, t2.b, t3.c;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "UNION");
            PlanTestBase.assertContains(plan, "test_mv1");
        }

        {
            String query = "select  t1.d, t2.b, t3.c, count(t1.a) " +
                        " from  iceberg0.partitioned_db.part_tbl1 as t1 " +
                        " inner join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d and t2.b != '' " +
                        " inner join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d " +
                        " where t1.d in ('2023-08-01', '2023-08-02') " +
                        " group by t1.d, t2.b, t3.c;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "UNION");
            PlanTestBase.assertContains(plan, "test_mv1");
        }

        starRocksAssert.dropMaterializedView(mvName);
    }

    @Test
    public void testStr2DateMVRefreshRewrite_LeftJoin_ExtraCompensate() throws Exception {
        String mvName = "test_mv1";
        starRocksAssert.withMaterializedView("create materialized view " + mvName + " " +
                    "partition by str2date(d,'%Y-%m-%d') " +
                    "distributed by hash(b) " +
                    "REFRESH DEFERRED MANUAL " +
                    "PROPERTIES (\n" +
                    "'replication_num' = '1'" +
                    ") " +
                    "as select  t1.d, t2.b, t3.c, count(t1.a) " +
                    " from  iceberg0.partitioned_db.part_tbl1 as t1 " +
                    " left join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d " +
                    " left join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d " +
                    " group by t1.d, t2.b, t3.c;");
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView materializedView = ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(testDb.getFullName(), mvName));

        // initial create
        starRocksAssert.getCtx().executeSql("refresh materialized view " + mvName + " partition start('2023-08-01') " +
                    "end ('2023-08-02') force with sync mode");
        List<String> partitions =
                    materializedView.getPartitions().stream().map(Partition::getName).sorted()
                                .collect(Collectors.toList());
        Assertions.assertEquals(Arrays.asList("p20230801_20230802"), partitions);

        connectContext.getSessionVariable().setMaterializedViewRewriteMode("default");
        {
            String query = "select  t1.d, t2.b, t3.c, count(t1.a) " +
                        " from  iceberg0.partitioned_db.part_tbl1 as t1 " +
                        " left join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d " +
                        " left join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d " +
                        " where t1.d='2023-08-01' " +
                        " group by t1.d, t2.b, t3.c;";

            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "0:OlapScanNode\n" +
                        "     TABLE: test_mv1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     partitions=1/1");
        }
        connectContext.getSessionVariable().setMaterializedViewRewriteMode("force");
        {
            String query = "select  t1.d, t2.b, t3.c, count(t1.a) " +
                        " from  iceberg0.partitioned_db.part_tbl1 as t1 " +
                        " left join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d " +
                        " left join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d " +
                        " where t1.d>='2023-08-01' " +
                        " group by t1.d, t2.b, t3.c;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "UNION");
            PlanTestBase.assertContains(plan, "13:OlapScanNode\n" +
                        "     TABLE: test_mv1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     partitions=1/1\n" +
                        "     rollup: test_mv1");
        }
        {
            String query = "select  t1.d, t2.b, t3.c, count(t1.a) " +
                        " from  iceberg0.partitioned_db.part_tbl1 as t1 " +
                        " left join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d " +
                        " left join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d " +
                        " where t1.d>='2023-08-01' and t2.b != '' and t2.b is not null " +
                        " group by t1.d, t2.b, t3.c;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "UNION");
            assertContainsMV(plan);
        }

        {
            String query = "select  t1.d, t2.b, t3.c, count(t1.a) " +
                        " from  iceberg0.partitioned_db.part_tbl1 as t1 " +
                        " left join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d " +
                        " left join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d " +
                        " where t1.d>='2023-08-01' and t2.b != '' and t2.b is not null and t2.b in ('xxx') " +
                        " and t3.c in ('xxx') " +
                        " group by t1.d, t2.b, t3.c;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "UNION");
            assertContainsMV(plan);
        }
        connectContext.getSessionVariable().setMaterializedViewRewriteMode("default");
        starRocksAssert.dropMaterializedView(mvName);
    }

    @Test
    public void testStr2DateMVRefreshRewriteInnerJoinAggregatePartialRefreshUnion() throws Exception {
        String mvName = "test_mv1";
        starRocksAssert.withMaterializedView("create materialized view " + mvName + " " +
                    "partition by str2date(d,'%Y-%m-%d') " +
                    "distributed by hash(a) " +
                    "REFRESH DEFERRED MANUAL " +
                    "PROPERTIES (\n" +
                    "'replication_num' = '1'" +
                    ") " +
                    "as select t1.a, t2.b, t1.d, count(t1.c)" +
                    " from  iceberg0.partitioned_db.part_tbl1 as t1 " +
                    " inner join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d " +
                    " inner join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d " +
                    " group by t1.a, t2.b, t1.d;");
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView materializedView = ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(testDb.getFullName(), mvName));

        // initial create
        starRocksAssert.getCtx().executeSql("refresh materialized view " + mvName + " partition start('2023-08-01') " +
                    "end ('2023-08-02') force with sync mode");
        List<String> partitions =
                    materializedView.getPartitions().stream().map(Partition::getName).sorted()
                                .collect(Collectors.toList());
        Assertions.assertEquals(Arrays.asList("p20230801_20230802"), partitions);
        connectContext.getSessionVariable().setMaterializedViewRewriteMode("force");

        {
            String query = "select t1.a, t2.b, t1.d, count(t1.c)\n" +
                        " from  iceberg0.partitioned_db.part_tbl1 as t1 \n" +
                        " inner join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d \n" +
                        " inner join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d \n" +
                        " where t1.d in ('2023-08-01')\n" +
                        " group by t1.a, t2.b, t1.d;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "0:OlapScanNode\n" +
                        "     TABLE: test_mv1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     partitions=1/1\n" +
                        "     rollup: test_mv1");
        }

        {
            String query = "select t1.a, t2.b, t1.d, count(t1.c)\n" +
                        " from  iceberg0.partitioned_db.part_tbl1 as t1 \n" +
                        " inner join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d \n" +
                        " inner join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d \n" +
                        " where t1.d ='2023-08-01'\n" +
                        " group by t1.a, t2.b, t1.d;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "0:OlapScanNode\n" +
                        "     TABLE: test_mv1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     partitions=1/1\n" +
                        "     rollup: test_mv1");
        }

        {
            String query = "select t1.a, t2.b, t1.d, count(t1.c)\n" +
                        " from  iceberg0.partitioned_db.part_tbl1 as t1 \n" +
                        " inner join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d \n" +
                        " inner join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d \n" +
                        " where t1.d in ('2023-08-01', '2023-08-02')\n" +
                        " group by t1.a, t2.b, t1.d;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "UNION");
            assertContainsMV(plan);
        }

        {
            String query = "select t1.a, t2.b, t1.d, count(t1.c)\n" +
                        " from  iceberg0.partitioned_db.part_tbl1 as t1 \n" +
                        " inner join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d \n" +
                        " inner join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d \n" +
                        " where t1.d in ('2023-08-01', '2023-08-02', '2023-08-03')\n" +
                        " group by t1.a, t2.b, t1.d;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "UNION");
            assertContainsMV(plan);
        }

        {
            String query = "select t1.a, t2.b, t1.d, count(t1.c)\n" +
                        " from  iceberg0.partitioned_db.part_tbl1 as t1 \n" +
                        " inner join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d \n" +
                        " inner join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d \n" +
                        " where t1.d >= '2023-08-01' \n" +
                        " group by t1.a, t2.b, t1.d;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "UNION");
            assertContainsMV(plan);
        }

        {
            String query = "select t1.a, t2.b, t1.d, count(t1.c)\n" +
                        " from  iceberg0.partitioned_db.part_tbl1 as t1 \n" +
                        " inner join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d \n" +
                        " inner join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d \n" +
                        " where t1.d >= '2023-08-01' and t1.d <= '2023-08-02' \n" +
                        " group by t1.a, t2.b, t1.d;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "UNION");
            assertContainsMV(plan);
        }

        {
            String query = "select t1.a, t2.b, t1.d, count(t1.c)\n" +
                        " from  iceberg0.partitioned_db.part_tbl1 as t1 \n" +
                        " inner join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d \n" +
                        " inner join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d \n" +
                        " where t1.d = '2023-08-01' or t1.d = '2023-08-02' \n" +
                        " group by t1.a, t2.b, t1.d;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "UNION");
            assertContainsMV(plan);
        }

        {
            String query = "select t1.a, t2.b, t1.d, count(t1.c)\n" +
                        " from  iceberg0.partitioned_db.part_tbl1 as t1 \n" +
                        " inner join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d \n" +
                        " inner join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d \n" +
                        " where t1.d in ('2023-08-01', '2023-08-02', '2023-08-03')\n" +
                        " group by t1.a, t2.b, t1.d;";
            connectContext.getSessionVariable().setRangePrunerPredicateMaxLen(0);
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "UNION");
            assertContainsMV(plan);
        }
        starRocksAssert.dropMaterializedView(mvName);
    }

    private static void assertContainsMV(String plan) {
        PlanTestBase.assertContains(plan, "     TABLE: test_mv1\n" +
                "     PREAGGREGATION: ON\n",
                "     partitions=1/1\n" +
                "     rollup: test_mv1");
    }

    @Test
    public void testStr2DateMVRefreshRewriteLeftJoinAggregatePartialRefreshUnion() throws Exception {
        String mvName = "test_mv1";
        starRocksAssert.withMaterializedView("create materialized view " + mvName + " " +
                    "partition by str2date(d,'%Y-%m-%d') " +
                    "distributed by hash(a) " +
                    "REFRESH DEFERRED MANUAL " +
                    "PROPERTIES (\n" +
                    "'replication_num' = '1'" +
                    ") " +
                    "as select t1.a, t2.b, t1.d, count(t1.c)" +
                    " from  iceberg0.partitioned_db.part_tbl1 as t1 " +
                    " left join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d " +
                    " left join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d " +
                    " group by t1.a, t2.b, t1.d;");
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView materializedView = ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(testDb.getFullName(), mvName));

        // initial create
        starRocksAssert.getCtx().executeSql("refresh materialized view " + mvName + " partition start('2023-08-01') " +
                    "end ('2023-08-02') force with sync mode");
        List<String> partitions =
                    materializedView.getPartitions().stream().map(Partition::getName).sorted()
                                .collect(Collectors.toList());
        Assertions.assertEquals(Arrays.asList("p20230801_20230802"), partitions);

        {
            String query = "select t1.a, t2.b, t1.d, count(t1.c)\n" +
                        " from  iceberg0.partitioned_db.part_tbl1 as t1 \n" +
                        " left join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d \n" +
                        " left join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d \n" +
                        " where t1.d in ('2023-08-01')\n" +
                        " group by t1.a, t2.b, t1.d;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "0:OlapScanNode\n" +
                        "     TABLE: test_mv1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     partitions=1/1\n" +
                        "     rollup: test_mv1");
        }

        {
            String query = "select t1.a, t2.b, t1.d, count(t1.c)\n" +
                        " from  iceberg0.partitioned_db.part_tbl1 as t1 \n" +
                        " left join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d \n" +
                        " left join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d \n" +
                        " where t1.d ='2023-08-01'\n" +
                        " group by t1.a, t2.b, t1.d;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "0:OlapScanNode\n" +
                        "     TABLE: test_mv1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     partitions=1/1\n" +
                        "     rollup: test_mv1");
        }

        {
            String query = "select t1.a, t2.b, t1.d, count(t1.c)\n" +
                        " from  iceberg0.partitioned_db.part_tbl1 as t1 \n" +
                        " left join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d \n" +
                        " left join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d \n" +
                        " where t1.d in ('2023-08-01', '2023-08-02')\n" +
                        " group by t1.a, t2.b, t1.d;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "UNION");
            assertContainsMV(plan);
        }

        {
            String query = "select t1.a, t2.b, t1.d, count(t1.c)\n" +
                        " from  iceberg0.partitioned_db.part_tbl1 as t1 \n" +
                        " left join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d \n" +
                        " left join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d \n" +
                        " where t1.d in ('2023-08-01', '2023-08-02', '2023-08-03')\n" +
                        " group by t1.a, t2.b, t1.d;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "UNION");
            assertContainsMV(plan);
        }

        {
            String query = "select t1.a, t2.b, t1.d, count(t1.c)\n" +
                        " from  iceberg0.partitioned_db.part_tbl1 as t1 \n" +
                        " left join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d \n" +
                        " left join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d \n" +
                        " where t1.d >= '2023-08-01' \n" +
                        " group by t1.a, t2.b, t1.d;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "UNION");
            assertContainsMV(plan);
        }

        {
            String query = "select t1.a, t2.b, t1.d, count(t1.c)\n" +
                        " from  iceberg0.partitioned_db.part_tbl1 as t1 \n" +
                        " left join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d \n" +
                        " left join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d \n" +
                        " where t1.d >= '2023-08-01' and t1.d <= '2023-08-02' \n" +
                        " group by t1.a, t2.b, t1.d;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "UNION");
            assertContainsMV(plan);
        }

        {
            String query = "select t1.a, t2.b, t1.d, count(t1.c)\n" +
                        " from  iceberg0.partitioned_db.part_tbl1 as t1 \n" +
                        " left join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d \n" +
                        " left join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d \n" +
                        " where t1.d = '2023-08-01' or t1.d = '2023-08-02' \n" +
                        " group by t1.a, t2.b, t1.d;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "UNION");
            assertContainsMV(plan);
        }

        {
            String query = "select t1.a, t2.b, t1.d, count(t1.c)\n" +
                        " from  iceberg0.partitioned_db.part_tbl1 as t1 \n" +
                        " left join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d \n" +
                        " left join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d \n" +
                        " where t1.d in ('2023-08-01', '2023-08-02', '2023-08-03')\n" +
                        " group by t1.a, t2.b, t1.d;";
            connectContext.getSessionVariable().setRangePrunerPredicateMaxLen(0);
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "UNION");
            assertContainsMV(plan);
        }
        starRocksAssert.dropMaterializedView(mvName);
    }

    @Test
    public void testStr2DateMVRefreshRewriteLeftJoinBitmapAggregatePartialRefreshUnion() throws Exception {
        String mvName = "test_mv1";
        starRocksAssert.withMaterializedView("create materialized view " + mvName + " " +
                    "partition by str2date(d,'%Y-%m-%d') " +
                    "distributed by hash(a) " +
                    "REFRESH DEFERRED MANUAL " +
                    "PROPERTIES (\n" +
                    "'replication_num' = '1'" +
                    ") " +
                    "as select t1.a, t2.b, t1.d, bitmap_union(to_bitmap(t1.c))" +
                    " from  iceberg0.partitioned_db.part_tbl1 as t1 " +
                    " left join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d " +
                    " left join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d " +
                    " group by t1.a, t2.b, t1.d;");
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView materializedView = ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(testDb.getFullName(), mvName));

        // initial create
        starRocksAssert.getCtx().executeSql("refresh materialized view " + mvName + " partition start('2023-08-01') " +
                    "end ('2023-08-02') force with sync mode");
        List<String> partitions =
                    materializedView.getPartitions().stream().map(Partition::getName).sorted()
                                .collect(Collectors.toList());
        Assertions.assertEquals(Arrays.asList("p20230801_20230802"), partitions);

        connectContext.getSessionVariable().setMaterializedViewRewriteMode("default");
        {
            String query = "select t1.a, t2.b, t1.d, count(distinct t1.c)\n" +
                        " from  iceberg0.partitioned_db.part_tbl1 as t1 \n" +
                        " left join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d \n" +
                        " left join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d \n" +
                        " where t1.d in ('2023-08-01')\n" +
                        " group by t1.a, t2.b, t1.d;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "test_mv1");
        }

        connectContext.getSessionVariable().setMaterializedViewRewriteMode("force");
        {
            String query = "select t1.a, t2.b, t1.d, count(distinct t1.c)\n" +
                        " from  iceberg0.partitioned_db.part_tbl1 as t1 \n" +
                        " left join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d \n" +
                        " left join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d \n" +
                        " group by t1.a, t2.b, t1.d;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "UNION");
            assertContainsMV(plan);
            PlanTestBase.assertContains(plan, "10:IcebergScanNode\n" +
                    "     TABLE: partitioned_db.part_tbl3\n" +
                    "     PREDICATES: 31: d IN ('2023-08-03', '2023-08-02')\n" +
                    "     MIN/MAX PREDICATES: 31: d >= '2023-08-02', 31: d <= '2023-08-03'");
        }

        {
            String query = "select t1.a, t2.b, t1.d, count(distinct t1.c)\n" +
                        " from  iceberg0.partitioned_db.part_tbl1 as t1 \n" +
                        " left join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d \n" +
                        " left join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d \n" +
                        " where t1.d ='2023-08-01'\n" +
                        " group by t1.a, t2.b, t1.d;";
            String plan = getFragmentPlan(query);
            assertContainsMV(plan);
        }

        {
            String query = "select t1.a, t2.b, t1.d, count(distinct t1.c)\n" +
                        " from  iceberg0.partitioned_db.part_tbl1 as t1 \n" +
                        " left join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d \n" +
                        " left join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d \n" +
                        " where t1.d in ('2023-08-01', '2023-08-02')\n" +
                        " group by t1.a, t2.b, t1.d;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "UNION");
            assertContainsMV(plan);
        }

        {
            String query = "select t1.a, t2.b, t1.d, count(distinct t1.c)\n" +
                        " from  iceberg0.partitioned_db.part_tbl1 as t1 \n" +
                        " left join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d \n" +
                        " left join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d \n" +
                        " where t1.d in ('2023-08-01', '2023-08-02', '2023-08-03')\n" +
                        " group by t1.a, t2.b, t1.d;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "UNION");
            assertContainsMV(plan);
        }

        {
            String query = "select t1.a, t2.b, t1.d, count(distinct t1.c)\n" +
                        " from  iceberg0.partitioned_db.part_tbl1 as t1 \n" +
                        " left join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d \n" +
                        " left join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d \n" +
                        " where t1.d >= '2023-08-01' \n" +
                        " group by t1.a, t2.b, t1.d;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "UNION");
            assertContainsMV(plan);
        }

        {
            String query = "select t1.a, t2.b, t1.d, count(distinct t1.c)\n" +
                        " from  iceberg0.partitioned_db.part_tbl1 as t1 \n" +
                        " left join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d \n" +
                        " left join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d \n" +
                        " where t1.d >= '2023-08-01' and t1.d <= '2023-08-02' \n" +
                        " group by t1.a, t2.b, t1.d;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "UNION");
            assertContainsMV(plan);
        }

        {
            String query = "select t1.a, t2.b, t1.d, count(distinct t1.c)\n" +
                        " from  iceberg0.partitioned_db.part_tbl1 as t1 \n" +
                        " left join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d \n" +
                        " left join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d \n" +
                        " where t1.d = '2023-08-01' or t1.d = '2023-08-02' \n" +
                        " group by t1.a, t2.b, t1.d;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "UNION");
            assertContainsMV(plan);
        }

        {
            String query = "select t1.a, t2.b, t1.d, count(distinct t1.c)\n" +
                        " from  iceberg0.partitioned_db.part_tbl1 as t1 \n" +
                        " left join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d \n" +
                        " left join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d \n" +
                        " where t1.d in ('2023-08-01', '2023-08-02', '2023-08-03')\n" +
                        " group by t1.a, t2.b, t1.d;";
            connectContext.getSessionVariable().setRangePrunerPredicateMaxLen(0);
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "UNION");
            assertContainsMV(plan);
        }
        connectContext.getSessionVariable().setMaterializedViewRewriteMode("default");
        starRocksAssert.dropMaterializedView(mvName);
    }

    @Test
    public void testStr2DateMVRefreshRewriteWithBitmapHash_LeftJoin() throws Exception {
        String mvName = "test_mv1";
        starRocksAssert.withMaterializedView("create materialized view " + mvName + " " +
                    "partition by str2date(d,'%Y-%m-%d') " +
                    "distributed by hash(b) " +
                    "REFRESH DEFERRED MANUAL " +
                    "PROPERTIES (\n" +
                    "'replication_num' = '1'" +
                    ") " +
                    "as select  t1.d, t2.b, t3.c, bitmap_union(bitmap_hash(t1.a)) " +
                    " from  iceberg0.partitioned_db.part_tbl1 as t1 " +
                    " left join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d " +
                    " left join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d " +
                    " group by t1.d, t2.b, t3.c;");
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView materializedView = ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(testDb.getFullName(), mvName));

        // initial create
        starRocksAssert.getCtx().executeSql("refresh materialized view " + mvName + " partition start('2023-08-01') " +
                    "end ('2023-08-02') force with sync mode");
        List<String> partitions =
                    materializedView.getPartitions().stream().map(Partition::getName).sorted()
                                .collect(Collectors.toList());
        Assertions.assertEquals(Arrays.asList("p20230801_20230802"), partitions);

        connectContext.getSessionVariable().setMaterializedViewRewriteMode("default");
        {
            String query = "select  t1.d, t2.b, t3.c, bitmap_union(bitmap_hash(t1.a)) " +
                        " from  iceberg0.partitioned_db.part_tbl1 as t1 " +
                        " left join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d " +
                        " left join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d " +
                        " where t1.d='2023-08-01' " +
                        " group by t1.d, t2.b, t3.c;";

            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "0:OlapScanNode\n" +
                        "     TABLE: test_mv1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     partitions=1/1");
        }

        {
            String query = "select  t1.d, t2.b, t3.c, bitmap_union_count(bitmap_hash(t1.a)) " +
                        " from  iceberg0.partitioned_db.part_tbl1 as t1 " +
                        " left join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d " +
                        " left join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d " +
                        " where t1.d='2023-08-01' " +
                        " group by t1.d, t2.b, t3.c;";

            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "0:OlapScanNode\n" +
                        "     TABLE: test_mv1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     partitions=1/1");
        }

        connectContext.getSessionVariable().setMaterializedViewRewriteMode("force");
        {
            String query = "select t1.d, t2.b, t3.c, count(distinct t1.a) " +
                        " from  iceberg0.partitioned_db.part_tbl1 as t1 " +
                        " left join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d " +
                        " left join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d " +
                        " where t1.d='2023-08-01' " +
                        " group by t1.d, t2.b, t3.c;";

            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "0:OlapScanNode\n" +
                        "     TABLE: test_mv1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     partitions=1/1");
        }

        {
            String query = "select  t1.d, t2.b, t3.c, count(distinct t1.a) " +
                        " from  iceberg0.partitioned_db.part_tbl1 as t1 " +
                        " left join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d " +
                        " left join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d " +
                        " where t1.d>='2023-08-01' " +
                        " group by t1.d, t2.b, t3.c;";

            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "UNION");
            PlanTestBase.assertContains(plan, "13:OlapScanNode\n" +
                        "     TABLE: test_mv1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     partitions=1/1");
        }

        {
            String query = "select  t1.d, count(distinct t1.a) " +
                        " from  iceberg0.partitioned_db.part_tbl1 as t1 " +
                        " left join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d " +
                        " left join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d " +
                        " where t1.d>='2023-08-01' " +
                        " group by t1.d;";

            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "UNION");
            assertContainsMV(plan);
        }
        starRocksAssert.dropMaterializedView(mvName);
        connectContext.getSessionVariable().setMaterializedViewRewriteMode("default");
    }

    @Test
    public void testStr2DateMVRefreshRewriteWithBitmapHash_InnerJoin() throws Exception {
        String mvName = "test_mv1";
        starRocksAssert.withMaterializedView("create materialized view " + mvName + " " +
                    "partition by str2date(d,'%Y-%m-%d') " +
                    "distributed by hash(b) " +
                    "REFRESH DEFERRED MANUAL " +
                    "PROPERTIES (\n" +
                    "'replication_num' = '1'" +
                    ") " +
                    "as select  t1.d, t2.b, t3.c, bitmap_union(bitmap_hash(t1.a)) " +
                    " from  iceberg0.partitioned_db.part_tbl1 as t1 " +
                    " inner join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d " +
                    " inner join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d " +
                    " group by t1.d, t2.b, t3.c;");
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView materializedView = ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(testDb.getFullName(), mvName));

        // initial create
        starRocksAssert.getCtx().executeSql("refresh materialized view " + mvName + " partition start('2023-08-01') " +
                    "end ('2023-08-02') force with sync mode");
        List<String> partitions =
                    materializedView.getPartitions().stream().map(Partition::getName).sorted()
                                .collect(Collectors.toList());
        Assertions.assertEquals(Arrays.asList("p20230801_20230802"), partitions);

        {
            String query = "select  t1.d, t2.b, t3.c, bitmap_union(bitmap_hash(t1.a)) " +
                        " from  iceberg0.partitioned_db.part_tbl1 as t1 " +
                        " inner join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d " +
                        " inner join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d " +
                        " where t1.d='2023-08-01' " +
                        " group by t1.d, t2.b, t3.c;";

            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "0:OlapScanNode\n" +
                        "     TABLE: test_mv1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     partitions=1/1");
        }

        {
            String query = "select  t1.d, t2.b, t3.c, bitmap_union_count(bitmap_hash(t1.a)) " +
                        " from  iceberg0.partitioned_db.part_tbl1 as t1 " +
                        " inner join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d " +
                        " inner join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d " +
                        " where t1.d='2023-08-01' " +
                        " group by t1.d, t2.b, t3.c;";

            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "0:OlapScanNode\n" +
                        "     TABLE: test_mv1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     partitions=1/1");
        }

        connectContext.getSessionVariable().setMaterializedViewRewriteMode("force");
        {
            String query = "select t1.d, t2.b, t3.c, count(distinct t1.a) " +
                        " from  iceberg0.partitioned_db.part_tbl1 as t1 " +
                        " inner join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d " +
                        " inner join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d " +
                        " where t1.d='2023-08-01' " +
                        " group by t1.d, t2.b, t3.c;";

            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "0:OlapScanNode\n" +
                        "     TABLE: test_mv1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     partitions=1/1");
        }

        {
            String query = "select  t1.d, t2.b, t3.c, count(distinct t1.a) " +
                        " from  iceberg0.partitioned_db.part_tbl1 as t1 " +
                        " inner join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d " +
                        " inner join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d " +
                        " where t1.d>='2023-08-01' " +
                        " group by t1.d, t2.b, t3.c;";

            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "UNION");
            PlanTestBase.assertContains(plan, "13:OlapScanNode\n" +
                        "     TABLE: test_mv1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     partitions=1/1");
        }

        {
            String query = "select  t1.d, count(distinct t1.a) " +
                        " from  iceberg0.partitioned_db.part_tbl1 as t1 " +
                        " inner join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d " +
                        " inner join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d " +
                        " where t1.d>='2023-08-01' " +
                        " group by t1.d;";

            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "UNION");
            PlanTestBase.assertContains(plan, "15:OlapScanNode\n" +
                        "     TABLE: test_mv1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     partitions=1/1");
        }
        connectContext.getSessionVariable().setMaterializedViewRewriteMode("default");
        starRocksAssert.dropMaterializedView(mvName);
    }

    @Test
    public void testStr2DateMVRefreshRewriteSingleTableWithDateTruc_Day() throws Exception {
        String mvName = "test_mv1";
        starRocksAssert.withView("CREATE VIEW view1 as select " +
                    " a, b, " +
                    " date_trunc('day', str2date(d,'%Y-%m-%d')) as dt, " +
                    " bitmap_union(to_bitmap(t1.c))\n" +
                    " from iceberg0.partitioned_db.part_tbl1 as t1 \n" +
                    " group by a, b, dt;");
        starRocksAssert.withMaterializedView("create materialized view " + mvName + " " +
                    "partition by dt " +
                    "distributed by hash(a) " +
                    "REFRESH DEFERRED MANUAL " +
                    "PROPERTIES (\n" +
                    "'replication_num' = '1'" +
                    ") " +
                    "as select * from view1");
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView materializedView = ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(testDb.getFullName(), mvName));

        // initial create
        starRocksAssert.getCtx().executeSql("refresh materialized view " + mvName + " partition start('2023-08-01') " +
                    "end ('2023-08-02') force with sync mode");
        List<String> partitions =
                    materializedView.getPartitions().stream().map(Partition::getName).sorted()
                                .collect(Collectors.toList());
        Assertions.assertEquals(Arrays.asList("p20230801_20230802"), partitions);

        connectContext.getSessionVariable().setMaterializedViewRewriteMode("force");
        {
            String query = "select a, b, date_trunc('day', str2date(d,'%Y-%m-%d')) as dt, " +
                        " count(distinct t1.c)\n" +
                        " from iceberg0.partitioned_db.part_tbl1 as t1 \n" +
                        " where date_trunc('day', str2date(d,'%Y-%m-%d'))='2023-08-01'" +
                        " group by a, b, dt;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "UNION");
            assertContainsMV(plan);
        }

        // TODO: Support date_trunc('day', str2date(t1.d, ''%Y-%m-%d'')) to str2date(d, '%Y-%m-%d')
        {
            String query = "select a, b, count(distinct t1.c)\n" +
                        " from iceberg0.partitioned_db.part_tbl1 as t1 \n" +
                        " where date_trunc('day', str2date(t1.d, '%Y-%m-%d'))  >= '2023-08-01' \n" +
                        " group by a, b;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "UNION");
            assertContainsMV(plan);
        }

        starRocksAssert.dropMaterializedView(mvName);
        starRocksAssert.dropView("view1");
        connectContext.getSessionVariable().setMaterializedViewRewriteMode("default");
    }

    @Test
    public void testStr2DateMVRefreshRewriteSingleTableWithDateTruc_Month() throws Exception {
        String mvName = "test_mv1";
        starRocksAssert.withView("CREATE VIEW view1 as select a, b, " +
                    " date_trunc('month', str2date(d,'%Y-%m-%d')) as dt, " +
                    " bitmap_union(to_bitmap(t1.c))\n" +
                    " from iceberg0.partitioned_db.part_tbl1 as t1 \n" +
                    " group by a, b, d;");
        starRocksAssert.withMaterializedView("create materialized view " + mvName + " " +
                    "partition by dt " +
                    "distributed by hash(a) " +
                    "REFRESH DEFERRED MANUAL " +
                    "PROPERTIES (\n" +
                    "'replication_num' = '1'" +
                    ") " +
                    "as select * from view1");
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView materializedView = ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(testDb.getFullName(), mvName));

        // initial create
        starRocksAssert.getCtx().executeSql("refresh materialized view " + mvName + " partition start('2023-08-01') " +
                    "end ('2023-08-02') force with sync mode");
        List<String> partitions =
                    materializedView.getPartitions().stream().map(Partition::getName).sorted()
                                .collect(Collectors.toList());
        Assertions.assertEquals(Arrays.asList("p202308_202309"), partitions);

        connectContext.getSessionVariable().setMaterializedViewRewriteMode("force");
        {
            String query = "select " +
                        " a, b, date_trunc('month', str2date(d,'%Y-%m-%d')) as dt, " +
                        " count(distinct t1.c)\n" +
                        " from iceberg0.partitioned_db.part_tbl1 as t1 \n" +
                        " where date_trunc('month', str2date(d,'%Y-%m-%d')) ='2023-08-01'" +
                        " group by a, b, dt;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertNotContains(plan, "UNION");
            UtFrameUtils.matchPlanWithoutId(plan, "0:OlapScanNode\n" +
                        "     TABLE: test_mv1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 9: dt = '2023-08-01'\n" +
                        "     partitions=1/1\n" +
                        "     rollup: test_mv1");
        }

        // TODO: Support date_trunc('day', str2date(t1.d, ''%Y-%m-%d'')) to str2date(d, '%Y-%m-%d')
        {
            String query = "select a, b, d, count(distinct t1.c)\n" +
                        " from iceberg0.partitioned_db.part_tbl1 as t1 \n" +
                        " where t1.d >= '2023-08-01' \n" +
                        " group by a, b, d;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertNotContains(plan, mvName);
        }

        starRocksAssert.dropMaterializedView(mvName);
        starRocksAssert.dropView("view1");
        connectContext.getSessionVariable().setMaterializedViewRewriteMode("default");
    }

    @Test
    public void testViewBasedRewrite() throws Exception {
        connectContext.getSessionVariable().setEnableViewBasedMvRewrite(true);
        String view = "create view iceberg_table_view " +
                    " as select t1.a, t2.b, t1.d, count(t1.c) as cnt" +
                    " from  iceberg0.partitioned_db.part_tbl1 as t1 " +
                    " inner join iceberg0.partitioned_db.part_tbl2 t2 on t1.d=t2.d " +
                    " inner join iceberg0.partitioned_db.part_tbl3 t3 on t1.d=t3.d " +
                    " group by t1.a, t2.b, t1.d;";
        starRocksAssert.withView(view);
        String mvName = "iceberg_mv_1";
        String mv = "create materialized view iceberg_mv_1 " +
                    "partition by str2date(d,'%Y-%m-%d') " +
                    "distributed by hash(a) " +
                    "REFRESH DEFERRED MANUAL " +
                    "PROPERTIES (\n" +
                    "'replication_num' = '1'" +
                    ") " +
                    "as " +
                    "select a, b, d, cnt " +
                    "from iceberg_table_view";
        starRocksAssert.withMaterializedView(mv);
        starRocksAssert.getCtx().executeSql("refresh materialized view iceberg_mv_1" +
                    " partition start('2023-08-01') end('2023-08-02') with sync mode");

        {
            String query = "select a, b, d, cnt from iceberg_table_view";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "iceberg_mv_1", "UNION");
        }
        {
            String query = "select a, b, d, cnt from iceberg_table_view where d = '2023-08-01'";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "iceberg_mv_1");
        }
        starRocksAssert.dropMaterializedView(mvName);
        connectContext.getSessionVariable().setEnableViewBasedMvRewrite(false);
    }

    @Test
    public void testListMVWithIcebergTable1() {
        final String mvName = "test_mv1";
        Config.enable_mv_list_partition_for_external_table = true;
        try {
            starRocksAssert.withMaterializedView("create materialized view " + mvName + " " +
                    "partition by str2date(d,'%Y-%m-%d') " +
                    "distributed by hash(a) " +
                    "REFRESH DEFERRED MANUAL " +
                    "PROPERTIES (\n" +
                    "'replication_num' = '1'" +
                    ") " +
                    "as select a, b, d, bitmap_union(to_bitmap(t1.c))" +
                    " from iceberg0.partitioned_db.part_tbl1 as t1 " +
                    " group by a, b, d;");
            Assertions.fail();
        } catch (Exception e) {
            Assertions.assertTrue(e.getMessage().contains("List partition expression can only be ref-base-table's " +
                    "partition expression but contain"));
        }
        Config.enable_mv_list_partition_for_external_table = false;
    }

    @Test
    public void testListMVWithIcebergTable2() throws Exception {
        Config.enable_mv_list_partition_for_external_table = true;
        final String mvName = "test_mv1";
        starRocksAssert.withMaterializedView("create materialized view " + mvName + " " +
                "partition by d " +
                "distributed by hash(a) " +
                "REFRESH DEFERRED MANUAL " +
                "PROPERTIES (\n" +
                "'replication_num' = '1'" +
                ") " +
                "as select a, b, d, bitmap_union(to_bitmap(t1.c))" +
                " from iceberg0.partitioned_db.part_tbl1 as t1 " +
                " group by a, b, d;");
        final MaterializedView mv = getMv(mvName);
        Assertions.assertTrue(mv.getPartitionInfo().isListPartition());
        Config.enable_mv_list_partition_for_external_table = false;
    }

    @Test
    public void testListMVWithIcebergTable3() throws Exception {
        String mvName = "test_mv1";
        Config.enable_mv_list_partition_for_external_table = true;
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW test_mv1\n" +
                "PARTITION BY date_trunc('month', ts)\n" +
                "DISTRIBUTED BY HASH(`id`) BUCKETS 10\n" +
                "REFRESH DEFERRED MANUAL\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")\n" +
                "AS SELECT id, data, ts  FROM `iceberg0`.`partitioned_transforms_db`.`t0_month` as a;");
        final MaterializedView mv = getMv(mvName);
        Assertions.assertTrue(mv.getPartitionInfo().isListPartition());
        Config.enable_mv_list_partition_for_external_table = false;
    }

    @Test
    public void testListMVWithIcebergTable4() throws Exception {
        String mvName = "test_mv1";
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW test_mv1\n" +
                "PARTITION BY date_trunc('month', ts)\n" +
                "DISTRIBUTED BY HASH(`id`) BUCKETS 10\n" +
                "REFRESH DEFERRED MANUAL\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")\n" +
                "AS SELECT id, data, ts  FROM `iceberg0`.`partitioned_transforms_db`.`t0_month` as a;");
        final MaterializedView mv = getMv(mvName);
        Assertions.assertTrue(mv.getPartitionInfo().isListPartition());
    }

    @Test
    public void testRefBaseTablePartitionWithTransform() throws Exception {
        final String sql = "CREATE MATERIALIZED VIEW test_mv1\n" +
                "PARTITION BY date_trunc('month', ts)\n" +
                "DISTRIBUTED BY HASH(`id`) BUCKETS 10\n" +
                "REFRESH DEFERRED MANUAL\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")\n" +
                "AS SELECT id, data, ts  FROM `iceberg0`.`partitioned_transforms_db`.`t0_month` as a;";
        CreateMaterializedViewStatement stmt =
                (CreateMaterializedViewStatement) UtFrameUtils.parseStmtWithNewParser(sql,
                        connectContext);
        MaterializedViewAnalyzer.analyze(stmt, starRocksAssert.getCtx());
        Assertions.assertTrue(stmt.isRefBaseTablePartitionWithTransform());
    }

    @Test
    public void testRewriteWithIcebergView1() throws Exception {
        starRocksAssert.withView("CREATE VIEW iceberg_view1 AS " +
                "SELECT id, data, ts FROM `iceberg0`.`partitioned_transforms_db`.`t0_month` order by id, ts;");
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW test_mv1\n" +
                "PARTITION BY date_trunc('month', ts)\n" +
                "REFRESH DEFERRED MANUAL\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")\n" +
                "AS SELECT * from iceberg_view1;");
        MaterializedView mv = getMv("test", "test_mv1");
        refreshMaterializedView("test", "test_mv1");
        List<BaseTableInfo> baseTableInfos = mv.getBaseTableInfos();
        Assertions.assertEquals(2, baseTableInfos.size());
        List<BaseTableInfo> baseTableInfosWithoutView = mv.getBaseTableInfosWithoutView();
        Assertions.assertEquals(1, baseTableInfosWithoutView.size());

        String query1 = "select * from iceberg_view1 limit 1;";
        String plan = getFragmentPlan(query1);
        PlanTestBase.assertContains(plan, "test_mv1");
        List<MvPlanContext> mvPlanContexts =
                CachingMvPlanContextBuilder.getInstance().getOrLoadPlanContext(mv, 3000);
        // mv's plan contexts should contain 2 entries:
        // - one is for view with inlined
        // - one is for view scan operator
        Assertions.assertTrue(mvPlanContexts.size() == 2);

        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW test_mv2\n" +
                "PARTITION BY date_trunc('month', ts)\n" +
                "REFRESH DEFERRED MANUAL\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")\n" +
                "AS SELECT * from test_mv1 where id > 100;");
        MaterializedView mv2 = getMv("test", "test_mv2");
        refreshMaterializedView("test", "test_mv2");
        List<BaseTableInfo> baseTableInfos2 = mv2.getBaseTableInfos();
        Assertions.assertEquals(1, baseTableInfos2.size());
        List<BaseTableInfo> baseTableInfosWithoutView2 = mv2.getBaseTableInfosWithoutView();
        Assertions.assertEquals(1, baseTableInfosWithoutView2.size());

        String query2 = "select * from test_mv1 where id > 100 limit 1;";
        String plan2 = getFragmentPlan(query2);
        PlanTestBase.assertContains(plan2, "test_mv2");
        mvPlanContexts = CachingMvPlanContextBuilder.getInstance().getOrLoadPlanContext(mv2, 3000);
        Assertions.assertTrue(mvPlanContexts.size() == 1);
    }
}
