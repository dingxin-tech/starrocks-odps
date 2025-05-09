---
displayed_sidebar: docs
sidebar_position: 10
---

# CBO 统计信息

本文介绍 StarRocks CBO 优化器（Cost-based Optimizer）的基本概念，以及如何为 CBO 优化器采集统计信息来优化查询计划。StarRocks 2.4 版本引入直方图作为统计信息，提供更准确的数据分布统计。

3.2 之前版本支持对 StarRocks 内表采集统计信息。从 3.2 版本起，支持采集 Hive，Iceberg，Hudi 表的统计信息，无需依赖其他系统。

## 什么是 CBO 优化器

CBO 优化器是查询优化的关键。一条 SQL 查询到达 StarRocks 后，会解析为一条逻辑执行计划，CBO 优化器对逻辑计划进行改写和转换，生成多个物理执行计划。通过估算计划中每个算子的执行代价（CPU、内存、网络、I/O 等资源消耗），选择代价最低的一条查询路径作为最终的物理查询计划。

StarRocks CBO 优化器采用 Cascades 框架，基于多种统计信息进行代价估算，能够在数万级别的执行计划中，选择代价最低的执行计划，提升复杂查询的效率和性能。StarRocks 1.16.0 版本推出自研的 CBO 优化器。1.19 版本及以上，该特性默认开启。

统计信息是 CBO 优化器的重要组成部分，统计信息的准确与否决定了代价估算是否准确，对于 CBO 选择最优 Plan 至关重要，进而决定了 CBO 优化器的性能好坏。下文详细介绍统计信息的类型、采集策略、以及如何创建采集任务和查看统计信息。

## 统计信息数据类型

StarRocks 会采集多种统计信息，为查询优化提供代价估算的参考。

### 基础统计信息

StarRocks 默认定期采集表和列的如下基础信息：

- row_count: 表的总行数

- data_size: 列的数据大小

- ndv: 列基数，即 distinct value 的个数

- null_count: 列中 NULL 值的个数

- min: 列的最小值

- max: 列的最大值

全量的统计信息存储在 StarRocks 集群 `_statistics_` 数据库下的 `column_statistics` 表中，您可以在 `_statistics_` 数据库下查看。查询时会返回类似如下信息：

```sql
SELECT * FROM _statistics_.column_statistics\G
*************************** 1. row ***************************
      table_id: 10174
  partition_id: 10170
   column_name: is_minor
         db_id: 10169
    table_name: zj_test.source_wiki_edit
partition_name: p06
     row_count: 2
     data_size: 2
           ndv: NULL
    null_count: 0
           max: 1
           min: 0
```

### 直方图

从 2.4 版本开始，StarRocks 引入直方图 (Histogram)。直方图常用于估算数据分布，当数据存在倾斜时，直方图可以弥补基础统计信息估算存在的偏差，输出更准确的数据分布信息。

StarRocks 采用等深直方图 (Equi-height Histogram)，即选定若干个 bucket，每个 bucket 中的数据量几乎相等。对于出现频次较高、对选择率（selectivity）影响较大的数值，StarRocks 会分配单独的桶进行存储。桶数量越多时，直方图的估算精度就越高，但是也会增加统计信息的内存使用。您可以根据业务情况调整直方图的桶个数和单个采集任务的 MCV（most common value）个数。

**直方图适用于有明显数据倾斜，并且有频繁查询请求的列。如果您的表数据分布比较均匀，可以不使用直方图。直方图支持的列类型为数值类型、DATE、DATETIME 或字符串类型。**

直方图统计信息存储在 StarRocks 集群 `_statistics_` 数据库的 `histogram_statistics` 表中。查询时会返回类似如下信息：

```sql
SELECT * FROM _statistics_.histogram_statistics\G
*************************** 1. row ***************************
   table_id: 10174
column_name: added
      db_id: 10169
 table_name: zj_test.source_wiki_edit
    buckets: NULL
        mcv: [["23","1"],["5","1"]]
update_time: 2023-12-01 15:17:10.274000
```

### 多列联合统计信息

自 v3.5.0 起，StarRocks 支持多列联合统计信息。当前 StarRocks 在进行基数估算（Cardinality Estimation）时，绝大部分场景优化器假设多个列之间是完全独立的，即没有相关性。但是，如果列之间存在相关性，当前的估算方法可能会导致错误的结果。这会导致优化器生成错误的执行计划。当前仅支持多列联合 NDV，基数估算时主要应用于以下场景。

- 评估多个 AND 连接的等值谓词。
- 评估 Agg 节点.
- 应用于聚合下推策略。

目前多列联合统计信息仅支持手动采集。手动采集时默认是抽样采集。多列联合统计信息存储在 StarRocks 集群 `_statistics_` 数据库的 `multi_column_statistics` 表中。查询时会返回类似如下信息：

```sql
mysql> select * from _statistics_.multi_column_statistics \G
*************************** 1. row ***************************
    table_id: 1695021
  column_ids: 0#1
       db_id: 110099
  table_name: db.test_multi_col_stats
column_names: id,name
         ndv: 11
 update_time: 2025-04-11 15:09:50
```

## 采集类型

随着表的导入或者删除操作，表的大小和数据分布会频繁更新，因此需要定期更新统计信息，以确保统计信息能准确反映表中的实际数据。在创建采集任务之前，您需要根据业务场景决定使用哪种采集类型和采集方式。

StarRocks 支持全量采集 (full collection) 和抽样采集 (sampled collection)。全量采集和抽样采集均支持自动和手动采集方式。

| **采集类型** | **采集方式** | **采集方法**                                                                                                                                                | **优缺点**                                                                          |
|----------|----------|---------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------|
| 全量采集     | 自动或手动    | 扫描全表，采集真实的统计信息值。按照分区（Partition）级别采集，基础统计信息的相应列会按照每个分区采集一条统计信息。如果对应分区无数据更新，则下次自动采集时，不再采集该分区的统计信息，减少资源占用。全量统计信息存储在 `_statistics_.column_statistics` 表中。   | 优点：统计信息准确，有利于优化器更准确地估算执行计划。缺点：消耗大量系统资源，速度较慢。从 2.4.5 版本开始支持用户配置自动采集的时间段，减少资源消耗。 |
| 抽样采集     | 自动或手动    | 从表的每个分区中均匀抽取 N 行数据，计算统计信息。抽样统计信息按照表级别采集，基础统计信息的每一列会存储一条统计信息。列的基数信息 (ndv) 是按照抽样样本估算的全局基数，和真实的基数信息会存在一定偏差。抽样统计信息存储在 `_statistics_.table_statistic_v1` 表中。 | 优点：消耗较少的系统资源，速度快。 缺点：统计信息存在一定误差，可能会影响优化器评估执行计划的准确性。                              |

自 v3.5.0 起，抽样和全量采集的统计信息均存储到 `_statistics_.column_statistics` 表中。因为当前优化器在基数估算时会以最近一次搜集的统计信息为主，而每次后台自动采集统计信息时可能因为表的健康度不同导致使用不同的采集方式。如果存在数据分布倾斜情况，全表抽样的统计信息误差率可能较高，相同查询可能因为全量和抽样采集方式不同导致使用的统计信息产生跳变，从而造成优化器生成错误的执行计划。因此抽样采集和全量采集承担统计信息均按照分区级别进行采集。您可以通过修改 FE 配置项 `statistic_use_meta_statistics` 为 `false` 以调整为先前的采集和存储方式。

## Predicate Column

自 v3.5.0 起，StarRocks 支持记录 Predicate Column。

Predicate Column 是指在查询中经常用于过滤条件（WHERE 子句、JOIN 条件、GROUP BY 列、DISTINCT 列）的列。StarRocks 会自动记录用户查询中涉及到的表的每个 Predicate Column，并将其存储在 `_statistics_.predicate_columns` 表中。 查询时会返回类似如下信息：

```sql
select * from _statistics_.predicate_columns \G
*************************** 1. row ***************************
      fe_id: 127.0.0.1_9011_1735874871718
      db_id: 1684773
   table_id: 1685786
  column_id: 1
      usage: normal,predicate,join,group_by
  last_used: 2025-04-11 20:39:32
    created: 2025-04-11 20:39:53
```

您也可以通过查询 `information_schema.column_stats_usage` 视图来获取更多可观测的信息。在极端场景（列数量较多）中，全量采集会存在较大的开销，而实际情况中，对于一个稳定的工作负载，往往并不需要所有列的统计信息，而只需要收集一些关键的 Filter、Join、Aggregation 中涉及到的列。因此为了权衡代价和准确度，StarRocks 支持手动采集 Predicate Column 统计信息，同时在自动搜集统计信息时根据策略来搜集 Predicate Column 统计信息，避免采集表中所有列统计信息。集群中每个 FE 节点会定时同步更新 Predicate Column 信息到 FE 缓存，方便使用时进行加速。

## 采集统计信息

StarRocks 提供灵活的信息采集方式，您可以根据业务场景选择自动采集、手动采集，也可以自定义自动采集任务。

默认情况下，StarRocks 会周期性自动采集表的全量统计信息。默认检查更新时间为 10 分钟一次，如果发现数据的更新比例满足条件，会自动触发采集。**全量采集可能会消耗大量的系统资源**，如果您不希望使用自动全量采集，可以设置 FE 配置项 `enable_collect_full_statistic` 为 `false`，系统的周期性采集任务会从全量采集方式改为抽样采集。

自 v3.5.0 起，自动采集过程中如果发现某张表自从上次采集到当前表中的数据变动较大，StarRocks 会自动将本次全量采集转为抽样采集。在命中抽样采集的情况下，如果表中存在 Predicate Column（PREDICATE/JOIN/GROUP BY/DISTINCT），StarRocks 会将抽样采集任务转为全量采集，收集 Predicate Column 的统计信息来保证统计信息的精确度，不会采集表中所有列的统计信息。您可以通过 FE 配置项 `statistic_auto_collect_use_full_predicate_column_for_sample` 进行配置。此外，如果命中全量采集的情况下，若表中列数超过 FE 配置项 `statistic_auto_collect_predicate_columns_threshold`，StarRocks 也会从全量采集所有列转为全量采集 Predicate Column。

### 自动采集 (Auto Collection)

对于基础统计信息，StarRocks 默认自动进行全表全量统计信息采集，无需人工操作。对于从未采集过统计信息的表，会在一个调度周期内自动进行统计信息采集。对于已经采集过统计信息的表，StarRocks 会自动更新表的总行数以及修改的行数，将这些信息定期持久化下来，作为是否触发自动采集的判断条件。当前自动采集任务不会对直方图和多列联合统计信息进行采集。

从 2.4.5 版本开始，支持用户配置自动全量采集的时间段，防止因集中采集而导致的集群性能抖动。采集时间段可通过 `statistic_auto_analyze_start_time` 和 `statistic_auto_analyze_end_time` 这两个 FE 配置项来配置。

在调度周期内，触发新的自动采集任务的条件：

- 上次统计信息采集之后，该表是否发生过数据变更。
- 采集落在配置的自动采集时间段内（默认为全天采集，可进行修改）。
- 最新一次统计信息采集任务的更新时间早于分区数据更新的时间。
- 该表的统计信息健康度（`statistic_auto_collect_ratio`）低于配置阈值。

> 健康度计算公式：
>
> 1. 当更新分区数量小于 10个时：1 - (上次统计信息采集后的更新行数/总行数)
> 2. 当更新分区数量大于等于 10 个时： 1 - MIN(上次统计信息采集后的更新行数/总行数, 上次统计信息采集后的更新的分区数/总分区数)
> 3. 自 v3.5.0 起，为判断一个分区是否健康，StarRocks 不再通过统计信息采集时间与数据更新时间来进行对比，而是通过分区更新行数的比例。您可以通过 FE 配置项 `statistic_partition_health_v2_threshold` 进行配置。同时，您也可以关闭FE配置项  `statistic_partition_healthy_v2` 来回退到之前的健康度检查行为。

同时，StarRocks 对于不同更新频率、不同大小的表，做了详细的配置策略。

- 对于数据量较小的表，**StarRocks 默认不做限制，即使表的更新频率很高，也会实时采集**。可以通过 `statistic_auto_collect_small_table_size` 配置小表的大小阈值，或者通过`statistic_auto_collect_small_table_interval` 配置小表的采集间隔。

- 对于数据量较大的表，StarRocks 按照以下策略限制：

  - 默认采集的间隔不低于 12 小时，通过 `statistic_auto_collect_large_table_interval` 配置。

  - 满足采集间隔的条件下，当健康度低于抽样采集阈值时，触发抽样采集，您可以通过 FE 配置项 `statistic_auto_collect_sample_threshold` 配置。自 v3.5.0 起，若命中以下所有条件，则抽样采集会转为对 Predicate Column 的全量采集：
    - 表中存在 Predicate Column, 且 Predicate Column 数量小于 `statistic_auto_collect_max_predicate_column_size_on_sample_strategy`。
    - `statistic_auto_collect_use_full_predicate_column_for_sample` 配置项为 `true`。

  - 满足采集间隔的条件下，健康度高于抽样采集阈值，低于采集阈值时，触发全量采集，通过 `statistic_auto_collect_ratio` 配置。

  - 当采集的分区数据总量大于 100G 时，触发抽样采集，通过 `statistic_max_full_collect_data_size` 配置。

  - 采集任务只会对分区更新时间晚于上次采集任务时间的分区进行采集，未发生修改的分区不进行采集。
- 对于存在 Predicate Column 并且总列数超过 `statistic_auto_collect_predicate_columns_threshold` 的表，仅会对表中 Predicate Column 进行统计信息采集。

:::tip

需要注意的是如果某张表的数据变更后，手动触发了对它的抽样采集任务会使得采样任务的更新时间晚于数据更新时间，不会在这一个调度周期内生成该表的全量采集任务。

:::

自动全量采集任务由系统自动执行，默认配置如下。您可以通过 [ADMIN SET CONFIG](../sql-reference/sql-statements/cluster-management/config_vars/ADMIN_SET_CONFIG.md) 命令修改。

配置项:

| **FE配置项**                                   | **类型**  | **默认值**      | **说明**                                              |
|---------------------------------------------|---------|--------------|-----------------------------------------------------|
| enable_statistic_collect                    | BOOLEAN | TRUE         | 是否开启默认的自动采集任务和用户自定义自动采任务。该参数默认打开。                                   |
| enable_auto_collect_statistics              | BOOLEAN | TRUE         | 是否开启默认的自动采集任务。该参数默认打开。                                   |
| enable_collect_full_statistic               | BOOLEAN | TRUE         | 是否开启自动全量统计信息采集。该参数默认打开。                             |
| statistic_collect_interval_sec              | LONG    | 600          | 自动定期任务中，检测数据更新的间隔，默认 10 分钟。单位：秒。                     |
| statistic_auto_analyze_start_time           | STRING  | 00:00:00     | 用于配置自动全量采集的起始时间。取值范围：`00:00:00` ~ `23:59:59`。       |
| statistic_auto_analyze_end_time             | STRING  | 23:59:59     | 用于配置自动全量采集的结束时间。取值范围：`00:00:00` ~ `23:59:59`。       |
| statistic_auto_collect_small_table_size     | LONG    | 5368709120   | 自动全量采集任务的小表阈值，默认 5 GB，单位：Byte。                         |
| statistic_auto_collect_small_table_interval | LONG    | 0            | 自动全量采集任务的小表采集间隔，单位：秒。                               |
| statistic_auto_collect_large_table_interval | LONG    | 43200        | 自动全量采集任务的大表采集间隔，默认 12 小时，单位：秒。                               |
| statistic_auto_collect_ratio                | DOUBLE  | 0.8          | 触发自动统计信息收集的健康度阈值。如果统计信息的健康度小于该阈值，则触发自动采集。           |
| statistic_auto_collect_sample_threshold     | DOUBLE  | 0.3          | 触发自动统计信息抽样收集的健康度阈值。如果统计信息的健康度小于该阈值，则触发自动抽样采集。       |
| statistic_max_full_collect_data_size        | LONG    | 107374182400 | 自动统计信息采集的单次任务最大数据量，默认 100 GB。单位：Byte。如果超过该值，则放弃全量采集，转为对该表进行抽样采集。 |
| statistic_full_collect_buffer               | LONG    | 20971520     | 自动全量采集任务写入的缓存大小，单位：Byte。默认值：20971520（20 MB）。                              |
| statistic_collect_too_many_version_sleep    | LONG    | 600000       | 当统计信息表的写入版本过多时 (Too many tablet 异常)，自动采集任务的休眠时间。单位：毫秒。默认值：600000（10 分钟）。 |
| statistic_auto_collect_use_full_predicate_column_for_sample | BOOLEAN    | TRUE       | 自动全量采集任务命中 Sample 策略时，是否将其转为对 Predicate Column 的全量采集。 |
| statistic_auto_collect_max_predicate_column_size_on_sample_strategy | INT    | 16       | 自动全量采集任务命中 Sample 策略时，如果表中 Predicate Column 特别多并且超过配置项，则不会转为对 Predicate Column 的全量采集，保持对所有列的抽样采集。当前配置项控制该行为中 Predicate Column 的最大值。 |
| statistic_auto_collect_predicate_columns_threshold | INT     | 32       | 自动采集时若发现表中的列数超过配置项，则仅会采集 Predicate Column 的列统计信息。 |
| statistic_predicate_columns_persist_interval_sec   | LONG    | 60       | FE 对 Predicate Column 的同步和持久化间隔周期。 |
| statistic_predicate_columns_ttl_hours       | LONG    | 24       | Predicate Column 信息在 FE 中缓存淘汰时间。 |

### 手动采集 (Manual Collection)

可以通过 ANALYZE TABLE 语句创建手动采集任务。**手动采集默认为同步操作。您也可以将手动任务设置为异步，执行命令后，系统会立即返回命令的状态，但是统计信息采集任务会异步在后台运行。异步采集适用于表数据量大的场景，同步采集适用于表数据量小的场景。手动任务创建后仅会执行一次，无需手动删除。运行状态可以使用 SHOW ANALYZE STATUS 查看**。创建手动采集任务，您需要被采集表的 INSERT 和 SELECT 权限。

#### 手动采集基础统计信息

```SQL
ANALYZE [FULL|SAMPLE] TABLE tbl_name 
    [( col_name [, col_name]... )
    | col_name [, col_name]...
    | ALL COLUMNS
    | PREDICATE COLUMNS
    | MULTIPLE COLUMNS ( col_name [, col_name]... )]
[PARTITION (partition_name [, partition_name]...)]
[WITH [SYNC | ASYNC] MODE]
[PROPERTIES (property [, property]...)]
```
参数说明：

- 采集类型
  - FULL：全量采集。
  - SAMPLE：抽样采集。
  - 如果不指定采集类型，默认为全量采集。

- `WITH SYNC | ASYNC MODE`: 如果不指定，默认为同步采集。

- 采集列类型：
  - `col_name`: 要采集统计信息的列，多列使用逗号分隔。如果不指定，表示采集整张表的信息。
  - `ALL COLUMNS`：对所有列进行采集。自 v3.5.0 起支持。
  - `PREDICATE COLUMNS`：仅对 Predicate Column 进行采集。自 v3.5.0 起支持。
  - `MULTIPLE COLUMNS`：对指定的多个列进行联合统计信息进行采集。当前多列联合统计信息仅支持手动同步采集。当前手动采集多列联合统计信息的列数不能超过 `statistics_max_multi_column_combined_num`, 默认值为 `10`。自 v3.5.0 起支持。

- `PROPERTIES`: 采集任务的自定义参数。如果不配置，则采用 `fe.conf` 中的默认配置。

| **PROPERTIES**                | **类型** | **默认值** | **说明**                           |
|-------------------------------|--------|---------|----------------------------------|
| statistic_sample_collect_rows | INT    | 200000  | 最小采样行数。如果参数取值超过了实际的表行数，默认进行全量采集。 |

**示例**

- 手动全量采集 (Manual full collection)

```SQL
-- 手动全量采集指定表的统计信息，使用默认配置。
ANALYZE TABLE tbl_name;

-- 手动全量采集指定表的统计信息，使用默认配置。
ANALYZE FULL TABLE tbl_name;

-- 手动全量采集指定表指定列的统计信息，使用默认配置。
ANALYZE TABLE tbl_name(c1, c2, c3);
```

- 手动抽样采集 (Manual sampled collection)

```SQL
-- 手动抽样采集指定表的统计信息，使用默认配置。
ANALYZE SAMPLE TABLE tbl_name;

-- 手动抽样采集指定表指定列的统计信息，设置抽样行数。
ANALYZE SAMPLE TABLE tbl_name (v1, v2, v3) PROPERTIES(
    "statistic_sample_collect_rows" = "1000000"
);
```

- 手动采集多列联合统计信息 (Manual multi-column combined collection)

```sql
-- 手动抽样采集指定表的多列联合统计信息
ANALYZE SAMPLE TABLE tbl_name MULTIPLE COLUMNS (v1, v2);

-- 手动全量采集指定表的多列联合统计信息
ANALYZE FULL TABLE tbl_name MULTIPLE COLUMNS (v1, v2);
```


- 手动采集 Predicate Column (Manual Predicate Column collection)

```sql
-- 手动抽样采集 Predicate Column
ANALYZE SAMPLE TABLE tbl_name PREDICATE COLUMNS

-- 手动全量采集 Predicate Column
ANALYZE FULL TABLE tbl_name PREDICATE COLUMNS
```

#### 手动采集直方图统计信息

```SQL
ANALYZE TABLE tbl_name UPDATE HISTOGRAM ON col_name [, col_name]
[WITH SYNC | ASYNC MODE]
[WITH N BUCKETS]
[PROPERTIES (property [,property])]
```

参数说明：

- `col_name`: 要采集统计信息的列，多列使用逗号分隔。该参数必填。

- `WITH SYNC | ASYNC MODE`: 如果不指定，默认为同步采集。

- `WITH N BUCKETS`: `N`为直方图的分桶数。如果不指定，则使用 `fe.conf` 中的默认值。

- PROPERTIES: 采集任务的自定义参数。如果不指定，则使用 `fe.conf` 中的默认配置。

| **PROPERTIES**                 | **类型** | **默认值**  | **说明**                           |
|--------------------------------|--------|----------|----------------------------------|
| statistic_sample_collect_rows  | INT    | 200000   | 最小采样行数。如果参数取值超过了实际的表行数，默认进行全量采集。 |
| histogram_mcv_size             | INT    | 100      | 直方图 most common value (MCV) 的数量。 |
| histogram_sample_ratio         | FLOAT  | 0.1      | 直方图采样比例。                         |
| histogram_buckets_size                      | LONG    | 64           | 直方图默认分桶数。                                                                                             |
| histogram_max_sample_row_count | LONG   | 10000000 | 直方图最大采样行数。                       |

直方图的采样行数由多个参数共同控制，采样行数取 `statistic_sample_collect_rows` 和表总行数 `histogram_sample_ratio` 两者中的最大值。最多不超过 `histogram_max_sample_row_count` 指定的行数。如果超过，则按照该参数定义的上限行数进行采集。

直方图任务实际执行中使用的 **PROPERTIES**，可以通过 SHOW ANALYZE STATUS 中的 **PROPERTIES** 列查看。

**示例**

```SQL
-- 手动采集v1列的直方图信息，使用默认配置。
ANALYZE TABLE tbl_name UPDATE HISTOGRAM ON v1;

-- 手动采集v1列的直方图信息，指定32个分桶，mcv指定为32个，采样比例为50%。
ANALYZE TABLE tbl_name UPDATE HISTOGRAM ON v1,v2 WITH 32 BUCKETS 
PROPERTIES(
   "histogram_mcv_size" = "32",
   "histogram_sample_ratio" = "0.5"
);
```

### 自定义自动采集 (Custom Collection)

#### 创建自动采集任务

StarRocks 提供的默认采集任务会自动对所有库和所有表根据策略进行统计信息采集，默认情况下您无需创建自定义采集任务。

如需自定义自动采集任务，需要通过 CREATE ANALYZE 语句创建。创建采集任务，您需要被采集表的 INSERT 和 SELECT 权限。

```SQL
-- 定期采集所有数据库的统计信息。
CREATE ANALYZE [FULL|SAMPLE] ALL [PROPERTIES (property [,property])]

-- 定期采集指定数据库下所有表的统计信息。
CREATE ANALYZE [FULL|SAMPLE] DATABASE db_name [PROPERTIES (property [,property])]

-- 定期采集指定表、列的统计信息。
CREATE ANALYZE [FULL|SAMPLE] TABLE tbl_name (col_name [,col_name]) [PROPERTIES (property [,property])]

-- 定期采集指定表、列的直方图信息。
CREATE ANALYZE TABLE tbl_name UPDATE HISTOGRAM ON col_name [, col_name]
[WITH SYNC | ASYNC MODE]
[WITH N BUCKETS]
[PROPERTIES (property [,property])]
```

参数说明：

- 采集类型
  - FULL：全量采集。
  - SAMPLE：抽样采集。
  - 如果不指定采集类型，默认为抽样采集。

- `col_name`: 要采集统计信息的列，多列使用逗号 (,)分隔。如果不指定，表示采集整张表的信息。

- `PROPERTIES`: 采集任务的自定义参数。如果不配置，则采用 `fe.conf` 中的默认配置。

| **PROPERTIES**                        | **类型** | **默认值** | **说明**                                                                                                                           |
|---------------------------------------|--------|---------|----------------------------------------------------------------------------------------------------------------------------------|
| statistic_auto_collect_ratio          | FLOAT  | 0.8     | 自动统计信息的健康度阈值。如果统计信息的健康度小于该阈值，则触发自动采集。                                                                                            |
| statistic_sample_collect_rows         | INT    | 200000  | 抽样采集的采样行数。如果该参数取值超过了实际的表行数，则进行全量采集。                                                                                              |
| statistic_exclude_pattern             | STRING | NULL    | 自动采集时，需要排除的库表，支持正则表达式匹配，匹配的格式为 `database.table`。                                                                                  |
| statistic_auto_collect_interval       | LONG   |  0      | 自动采集的间隔时间，单位：秒。StarRocks 默认根据表大小选择使用 `statistic_auto_collect_small_table_interval` 或者 `statistic_auto_collect_large_table_interval`。如果创建 Analyze 任务时在 PROPERTIES 中指定了 `statistic_auto_collect_interval`，则直接按照该值的间隔执行 Analyze 任务，而不用依据 `statistic_auto_collect_small_table_interval` 或 `statistic_auto_collect_large_table_interval` 参数。|

**示例**

**自动全量采集**

```SQL
-- 定期全量采集所有数据库的统计信息。
CREATE ANALYZE ALL;

-- 定期全量采集指定数据库下所有表的统计信息。
CREATE ANALYZE DATABASE db_name;

-- 定期全量采集指定数据库下所有表的统计信息。
CREATE ANALYZE FULL DATABASE db_name;

-- 定期全量采集指定表、列的统计信息。
CREATE ANALYZE TABLE tbl_name(c1, c2, c3); 
    
-- 定期全量采集所有数据库的统计信息，不收集`db_name`数据库。
CREATE ANALYZE ALL PROPERTIES (
   "statistic_exclude_pattern" = "db_name\."
);    
```

**自动抽样采集**

```SQL
-- 定期抽样采集指定数据库下所有表的统计信息。
CREATE ANALYZE SAMPLE DATABASE db_name;

-- 定期抽样采集指定表、列的统计信息，设置采样行数和健康度阈值。
CREATE ANALYZE SAMPLE TABLE tbl_name(c1, c2, c3) PROPERTIES(
   "statistic_auto_collect_ratio" = "0.5",
   "statistic_sample_collect_rows" = "1000000"
);
    
-- 自动采集所有数据库的统计信息，不收集`db_name.tbl_name`表。
CREATE ANALYZE SAMPLE DATABASE db_name PROPERTIES (
   "statistic_exclude_pattern" = "db_name.tbl_name"
);

-- 定期抽样采集指定库、表、列的直方图信息。
CREATE ANALYZE TABLE tbl_name UPDATE HISTOGRAM ON c1,c2;
```

**无需 StarRocks 提供的自动采集任务，使用用户自定义采集任务，且不搜集 `db_name.tbl_name` 表**

```sql
ADMIN SET FRONTEND CONFIG("enable_auto_collect_statistics"="false");
DROP ALL ANALYZE JOB;
CREATE ANALYZE FULL ALL db_name PROPERTIES (
   "statistic_exclude_pattern" = "db_name.tbl_name"
);
```

### 导入数据时采集统计信息

为保证针对数据导入后立即查询产生相对较好的执行计划，StarRocks 会在 INSERT INTO/OVERWRITE 两种 DML 语句执行结束后，触发异步的统计信息采集任务。DML 结束后默认等待 30 秒。如果统计信息采集任务 30 秒没有结束，则返回 DML 执行结果。

#### INSERT INTO

- 仅会对分区首次导入数据进行统计信息采集。
- 如果本次导入的数据行数大于 `statistic_sample_collect_rows`，则触发抽样采集任务，否则使用全量采集。

#### INSERT OVERWRITE

- 如果 OVERWRITE 前后行数变动比例小于 `statistic_sample_collect_ratio_threshold_of_first_load`，则不会触发统计信息采集任务。
- 如果本次 OVERWRITE 行数大于 `statistic_sample_collect_rows`，则触发抽样采集任务，否则使用全量采集。
  
以下为针对导入数据的创建自定义采集任务的属性（PROPERTIES）。如果不配置，则采用对应 FE 配置项的值。

| **PROPERTIES**                        | **类型** | **默认值** | **说明**                                                                                                                           |
|-------------------------------------------|--------|---------|---------------------------------------------------------------------------------------------------------------------------- |
| enable_statistic_collect_on_first_load    | BOOLEAN  | TRUE     | 执行 INSERT INTO/OVERWRITE 后是否触发统计信息采集任务。                                                                       |
| semi_sync_collect_statistic_await_seconds | LONG    | 30     | 返回结果前等待统计信息采集的最长时间。                                                                  |
| statistic_sample_collect_ratio_threshold_of_first_load | DOUBLE    | 0.1  | OVERWRITE 不触发统计信息搜集任务的数据变动比例。                                                               |
| statistic_sample_collect_rows             | LONG | 200000    | DML 导入总数据量超过该值时，使用抽样采集统计信息。                                                                               |

#### 查看自动采集任务

```SQL
SHOW ANALYZE JOB [WHERE predicate][ORDER BY columns][LIMIT num]
```

您可以使用 WHERE 子句设定筛选条件，进行返回结果筛选。该语句会返回如下列。

| **列名**       | **说明**                                                         |
|--------------|----------------------------------------------------------------|
| Id           | 采集任务的ID。                                                       |
| Database     | 数据库名。                                                          |
| Table        | 表名。                                                            |
| Columns      | 列名列表。                                                          |
| Type         | 统计信息的类型。取值： FULL，SAMPLE。                                       |
| Schedule     | 调度的类型。自动采集任务固定为 `SCHEDULE`。                                    |
| Properties   | 自定义参数信息。                                                       |
| Status       | 任务状态，包括 PENDING（等待）、RUNNING（正在执行）、SUCCESS（执行成功）和 FAILED（执行失败）。 |
| LastWorkTime | 最近一次采集时间。                                                      |
| Reason       | 任务失败的原因。如果执行成功则为 `NULL`。                                       |

**示例**

```SQL
-- 查看集群全部自定义采集任务。
SHOW ANALYZE JOB

-- 查看数据库test下的自定义采集任务。
SHOW ANALYZE JOB where `database` = 'test';
```

#### 删除自动采集任务

```SQL
DROP ANALYZE <ID>
| DROP ALL ANALYZE JOB
```

**示例**

```SQL
DROP ANALYZE 266030;
```

```SQL
DROP ALL ANALYZE JOB;
```

## 查看采集任务状态

您可以通过 SHOW ANALYZE STATUS 语句查看当前所有采集任务的状态。该语句不支持查看自定义采集任务的状态，如要查看，请使用 SHOW ANALYZE JOB。

```SQL
SHOW ANALYZE STATUS [LIKE | WHERE predicate][ORDER BY columns][LIMIT num]
```

您可以使用 `Like` 或 `Where` 来筛选需要返回的信息。

目前 SHOW ANALYZE STATUS 会返回如下列。

| **列名**   | **说明**                                                     |
| ---------- | ------------------------------------------------------------ |
| Id         | 采集任务的ID。                                               |
| Database   | 数据库名。                                                   |
| Table      | 表名。                                                       |
| Columns    | 列名列表。                                                   |
| Type       | 统计信息的类型，包括 FULL，SAMPLE，HISTOGRAM。               |
| Schedule   | 调度的类型。`ONCE` 表示手动，`SCHEDULE` 表示自动。             |
| Status     | 任务状态，包括 RUNNING（正在执行）、SUCCESS（执行成功）和 FAILED（执行失败）。 |
| StartTime  | 任务开始执行的时间。                                         |
| EndTime    | 任务结束执行的时间。                                         |
| Properties | 自定义参数信息。                                             |
| Reason     | 任务失败的原因。如果执行成功则为 NULL。                      |

## 查看统计信息

### 基础统计信息元数据

```SQL
SHOW STATS META [WHERE predicate][ORDER BY columns][LIMIT num]
```

该语句返回如下列。

| **列名**   | **说明**                                            |
| ---------- | --------------------------------------------------- |
| Database   | 数据库名。                                          |
| Table      | 表名。                                              |
| Columns    | 列名。                                              |
| Type       | 统计信息的类型，`FULL` 表示全量，`SAMPLE` 表示抽样。 |
| UpdateTime | 当前表的最新统计信息更新时间。                      |
| Properties | 自定义参数信息。                                    |
| Healthy    | 统计信息健康度。                                    |
| ColumnStats  | 列 ANALYZE 类型。                                 |
| TabletStatsReportTime | 表的 Tablet 元数据在 FE 的更新时间。       |
| TableHealthyMetrics    | 统计信息健康度指标。                       |
| TableUpdateTime    | Table 更新时间。                          |

### 直方图统计信息元数据

```SQL
SHOW HISTOGRAM META [WHERE predicate]
```

该语句返回如下列。

| **列名**   | **说明**                                  |
| ---------- | ----------------------------------------- |
| Database   | 数据库名。                                |
| Table      | 表名。                                    |
| Column     | 列名。                                    |
| Type       | 统计信息的类型，直方图固定为 `HISTOGRAM`。 |
| UpdateTime | 当前表的最新统计信息更新时间。            |
| Properties | 自定义参数信息。                          |

## 删除统计信息

StarRocks 支持手动删除统计信息。手动删除统计信息时，会删除统计信息数据和统计信息元数据，并且会删除过期内存中的统计信息缓存。需要注意的是，如果当前存在自动采集任务，可能会重新采集之前已删除的统计信息。您可以使用 SHOW ANALYZE STATUS 查看统计信息采集历史记录。

### 删除基础统计信息

以下命令将删除 `default_catalog._statistics_.column_statistics` 表中存储的统计信息，FE 缓存的对应表统计信息也将失效。同时从 v3.5.0 起，该命令会同时删除该表的多列联合统计信息。

```SQL
DROP STATS tbl_name
```

以下命令将删除 `default_catalog._statistics_.multi_column_statistics` 表中存储的多列联合统计信息，FE 缓存的对应表中的多列联合统计信息也将失效。该命令不会删除表的基础统计信息。

```SQL
DROP MULTIPLE COLUMNS STATS tbl_name
```

### 删除直方图统计信息

```SQL
ANALYZE TABLE tbl_name DROP HISTOGRAM ON col_name [, col_name]
```

## 取消采集任务

您可以通过 KILL ANALYZE 语句取消正在运行中（Running）的统计信息采集任务，包括手动采集任务和自定义自动采集任务。

手动采集任务的任务 ID 可以在 SHOW ANALYZE STATUS 中查看。自定义自动采集任务的任务 ID 可以在 SHOW ANALYZE JOB 中查看。

```SQL
KILL ANALYZE <ID>
```

## 其他 FE 配置项

| **FE 配置项**        | **类型** | **默认值** | **说明**                                              |
| ------------------------------------ | -------- | ----------------- | ------------------------------------------------------------ |
| statistic_manager_sleep_time_sec            | LONG    | 60           | 统计信息相关元数据调度间隔周期。单位：秒。系统根据这个间隔周期，来执行如下操作：<ul><li>创建统计信息表；</li><li>删除已经被删除的表的统计信息；</li><li>删除过期的统计信息历史记录。</li></ul> |
| statistic_analyze_status_keep_second        | LONG    | 259200       | 采集任务记录保留时间，默认为 3 天。单位：秒。                                                                                          |

## 系统变量

`statistic_collect_parallel` 用于调整 BE 上能并发执行的统计信息收集任务的个数，默认值为 1，可以调大该数值来加快收集任务的执行速度。

## 采集外表的统计信息

从 3.2 版本起，支持采集 Hive、Iceberg、Hudi 表的统计信息。**采集的语法和内表相同，但是只支持手动全量采集、手动直方图采集（自 v3.2.7 起）、自动全量采集，不支持抽样采集**。自 v3.3.0 起，支持采集 Delta Lake 表的统计信息，并支持采集 STRUCT 子列的统计信息。自 v3.4.0 起，支持通过查询触发 ANALYZE 任务自动收集统计信息。

收集的统计信息会写入到 `_statistics_` 数据库的 `external_column_statistics` 表中，不会写入到 Hive Metastore 中，因此无法和其他查询引擎共用。您可以通过查询 `default_catalog._statistics_.external_column_statistics` 表中是否写入了表的统计信息。

查询时，会返回如下信息：

```sql
SELECT * FROM _statistics_.external_column_statistics\G
*************************** 1. row ***************************
    table_uuid: hive_catalog.tn_test.ex_hive_tbl.1673596430
partition_name: 
   column_name: k1
  catalog_name: hive_catalog
       db_name: tn_test
    table_name: ex_hive_tbl
     row_count: 3
     data_size: 12
           ndv: NULL
    null_count: 0
           max: 3
           min: 2
   update_time: 2023-12-01 14:57:27.137000
```

### 使用限制

对外表采集统计信息时，有如下限制：

- 目前只支持采集 Hive、Iceberg、Hudi、Delta Lake（自 v3.3.0 起） 表的统计信息。
- 目前只支持手动全量采集、手动直方图采集（自 v3.2.7 起）、自动全量采集、查询触发采集（自 v3.4.0 起），不支持抽样采集。
- 全量自动采集，需要创建一个采集任务，系统不会默认自动采集外部数据源的统计信息。
- 对于自动采集任务：
  - 只支持采集指定表的统计信息，不支持采集所有数据库、数据库下所有表的统计信息。
  - 目前只有 Hive 和 Iceberg 表可以每次检查数据是否发生更新，数据发生了更新才会执行采集任务, 并且只会采集数据发生了更新的分区。Hudi 表目前无法判断是否发生了数据更新，所以会根据采集间隔周期性全表采集。
- 对于查询触发采集：
  - 目前只有 Leader FE 节点可以触发收集任务。
  - 仅支持检查 Hive、Iceberg 外表的分区变动，只收集数据发生变动分区的统计信息。对于 Delta Lake/Hudi 外表，系统会收集整表的统计信息。
  - 如果 Iceberg 表启用 Partition Transform，仅支持对于 `identity`、`year`、`month`、`day`、`hour` 类型 Transform 收集统计信息。
  - 不支持针对 Iceberg 表的 Partition Evolution 收集统计信息。

以下示例默认在 External Catalog 指定数据库下采集表的统计信息。如果是在 `default_catalog` 下采集 External Catalog 下表的统计信息，引用表名时可以使用 `[catalog_name.][database_name.]<table_name>` 格式。

### 查询触发采集

自 v3.4.0 起，支持通过查询触发 ANALYZE 任务自动收集外表的统计信息。当查询 Hive、Iceberg、Hudi、Delta Lake 表时，系统会在后台自动触发 ANALYZE 任务，收集对应表和列的统计信息，可以用于后续查询计划优化。

触发流程：

1. 优化器查询 FE 缓存的统计信息时，会依据被查询的表和列确定需要触发的 ANALYZE 任务的对象（ANALYZE 任务只会收集查询中包含的列的统计信息）。
2. 系统会将任务对象包装为一个 ANALYZE 任务加入 PendingTaskQueue 中。
3. Schedule 线程会周期性从 PendingTaskQueue 中获取任务放入 RunningTasksQueue 中执行。
4. ANALYZE 任务被执行时会收集统计信息并写入到 BE 中，并清除 FE 中缓存的过期统计信息。

该功能默认开启。你可以通过以下系统变量和配置项控制以上流程。

#### 系统变量

##### enable_query_trigger_analyze

- 默认值：true
- 类型：Boolean
- 单位：-
- 描述：是否开启查询触发 ANALYZE 任务。
- 引入版本：v3.4.0

#### FE 配置项

##### connector_table_query_trigger_analyze_small_table_rows

- 默认值：10000000
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：查询触发 ANALYZE 任务的小表阈值。
- 引入版本：v3.4.0

##### connector_table_query_trigger_analyze_small_table_interval

- 默认值：2 * 3600
- 类型：Int
- 单位：秒
- 是否动态：是
- 描述：查询触发 ANALYZE 任务的小表采集间隔。
- 引入版本：v3.4.0

##### connector_table_query_trigger_analyze_large_table_interval

- 默认值：12 * 3600
- 类型：Int
- 单位：秒
- 是否动态：是
- 描述：查询触发 ANALYZE 任务的大表采集间隔。
- 引入版本：v3.4.0

##### connector_table_query_trigger_analyze_max_pending_task_num

- 默认值：100
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：FE 中处于 Pending 状态的查询触发 ANALYZE 任务的最大数量。
- 引入版本：v3.4.0

##### connector_table_query_trigger_analyze_schedule_interval

- 默认值：30
- 类型：Int
- 单位：秒
- 是否动态：是
- 描述：Schedule 线程调度查询触发 ANALYZE 任务的周期。
- 引入版本：v3.4.0

##### connector_table_query_trigger_analyze_max_running_task_num

- 默认值：2
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：FE 中处于 Running 状态的查询触发 ANALYZE 任务的最大数量。
- 引入版本：v3.4.0

### 手动采集

#### 创建手动采集任务

语法：

```sql
-- 手动全量采集
ANALYZE [FULL] TABLE tbl_name (col_name [,col_name])
[WITH SYNC | ASYNC MODE]
[PROPERTIES(property [,property])]

-- 手动直方图采集（自 v3.3.0 起）
ANALYZE TABLE tbl_name UPDATE HISTOGRAM ON col_name [, col_name]
[WITH SYNC | ASYNC MODE]
[WITH N BUCKETS]
[PROPERTIES (property [,property])]
```

以下示例以手动全量采集为例：

```sql
ANALYZE TABLE ex_hive_tbl(k1);
+----------------------------------+---------+----------+----------+
| Table                            | Op      | Msg_type | Msg_text |
+----------------------------------+---------+----------+----------+
| hive_catalog.tn_test.ex_hive_tbl | analyze | status   | OK       |
+----------------------------------+---------+----------+----------+
```

#### 查看 Analyze 任务执行状态

语法：

```sql
SHOW ANALYZE STATUS [LIKE | WHERE predicate]
```

示例：

```sql
SHOW ANALYZE STATUS where `table` = 'ex_hive_tbl';
+-------+----------------------+-------------+---------+------+----------+---------+---------------------+---------------------+------------+--------+
| Id    | Database             | Table       | Columns | Type | Schedule | Status  | StartTime           | EndTime             | Properties | Reason |
+-------+----------------------+-------------+---------+------+----------+---------+---------------------+---------------------+------------+--------+
| 16400 | hive_catalog.tn_test | ex_hive_tbl | k1      | FULL | ONCE     | SUCCESS | 2023-12-04 16:31:42 | 2023-12-04 16:31:42 | {}         |        |
| 16465 | hive_catalog.tn_test | ex_hive_tbl | k1      | FULL | ONCE     | SUCCESS | 2023-12-04 16:37:35 | 2023-12-04 16:37:35 | {}         |        |
| 16467 | hive_catalog.tn_test | ex_hive_tbl | k1      | FULL | ONCE     | SUCCESS | 2023-12-04 16:37:46 | 2023-12-04 16:37:46 | {}         |        |
+-------+----------------------+-------------+---------+------+----------+---------+---------------------+---------------------+------------+--------+
```

#### 查看统计信息元数据

查看表的统计信息元数据。

语法：

```sql
SHOW STATS META [WHERE predicate]
```

示例：

```sql
SHOW STATS META where `table` = 'ex_hive_tbl';
+----------------------+-------------+---------+------+---------------------+------------+---------+
| Database             | Table       | Columns | Type | UpdateTime          | Properties | Healthy |
+----------------------+-------------+---------+------+---------------------+------------+---------+
| hive_catalog.tn_test | ex_hive_tbl | k1      | FULL | 2023-12-04 16:37:46 | {}         |         |
+----------------------+-------------+---------+------+---------------------+------------+---------+
```

#### 取消采集任务

取消正在运行中（Running）的统计信息采集任务。

语法：

```sql
KILL ANALYZE <ID>
```

任务 ID 可以在 SHOW ANALYZE STATUS 中查看。

### 自动采集

对于外部数据源中的表，需要创建一个自动采集任务，StarRocks 会根据采集任务中指定的属性，周期性检查采集任务是否需要执行，默认检查时间为 5 min。Hive 和 Iceberg 仅在发现有数据更新时，才会自动执行一次采集任务。

StarRocks 目前不支持感知 Hudi 数据更新，所以只能周期性采集统计数据。您可以指定以下 FE 配置项来控制收集行为：

- statistic_collect_interval_sec

  自动定期任务中，检查是否需要采集统计信息的间隔周期，默认 5 分钟。

- statistic_auto_collect_small_table_rows

  自动采集中，用于判断外部数据源下的表 (Hive, Iceberg, Hudi) 是否为小表的行数门限，默认值为 10000000 行。该参数 3.2 版本引入。

- statistic_auto_collect_small_table_interval

  自动采集中小表的采集间隔，单位：秒。默认值：0。

- statistic_auto_collect_large_table_interval

  自动采集中大表的采集间隔，单位：秒。默认值：43200 (12 小时)。

自动采集线程每间隔 `statistic_collect_interval_sec` 时间就会进行一次任务检查，发现是否有需要执行的任务，当表中行数小于 `statistic_auto_collect_small_table_rows` 时，则将采集间隔设置为小表的采集间隔，否则设置为大表的采集间隔。如果 `上次更新时间 + 采集间隔 > 当前时间`，则需要更新，否则无需更新，这样可以避免频繁为大表执行 Analyze 任务。

#### 创建自动采集任务

语法：

```sql
CREATE ANALYZE TABLE tbl_name (col_name [,col_name])
[PROPERTIES (property [,property])]
```

您可以通过 Property `statistic_auto_collect_interval` 为当前自动收集任务单独设置收集间隔。此时 FE 配置项 `statistic_auto_collect_small_table_interval` 和 `statistic_auto_collect_large_table_interval` 将不会对该任务生效。

示例：

```sql
CREATE ANALYZE TABLE ex_hive_tbl (k1)
PROPERTIES ("statistic_auto_collect_interval" = "5");

Query OK, 0 rows affected (0.01 sec)
```

示例：

#### 查看 Analyze 任务执行状态

同手动采集。

#### 查看统计信息元数据

同手动采集。

#### 查看自动采集任务

语法：

```sql
SHOW ANALYZE JOB [WHERE predicate]
```

示例：

```sql
SHOW ANALYZE JOB WHERE `id` = '17204';

Empty set (0.00 sec)
```

#### 取消采集任务

同手动采集。

#### 删除 Hive/Iceberg/Hudi 表统计信息

```sql
DROP STATS tbl_name
```

## 更多信息

- 如需查询 FE 配置项的取值，执行 [ADMIN SHOW CONFIG](../sql-reference/sql-statements/cluster-management/config_vars/ADMIN_SHOW_CONFIG.md)。

- 如需修改 FE 配置项的取值，执行 [ADMIN SET CONFIG](../sql-reference/sql-statements/cluster-management/config_vars/ADMIN_SET_CONFIG.md)。
