IndexR 使用手册
====

## 1 简介

IndexR 是一个数据库系统，在使用上可以分成两个部分，一个是作为 Hive 的文件格式，一个作为 Drill 的插件。它的数据内容存放在 HDFS 上面，可以使用 Hive 管理数据，比如导入导出、删除修改等，也可以用 Hive 直接查询。不过一般查询是通过 Drill ，可以实现数据的准实时、交互式 OLAP 查询。

## 2 表管理


### 2.1 创建表

IndexR 的表目前需要两次定义，创建 Hive 表和创建 IndexR 表。目前 IndexR 只支持 `int`, `long`（对应Hive `bigint`）, `float`, `double`, `string` 四种数据类型。

创建 Hive 表的目的是为了可以方便的使用 Hive 进行数据管理。理论上不一定需要 Hive 表，可以直接通过 HDFS 的操作也可以达到管理数据的目的。

#### 2.1.1 创建 Hive 表

打开 Hive 的命令行界面

```
[flow@localhost:~]$ cd /usr/local/hive
[flow@localhost:/usr/local/hive]$ bin/hive
hive (default)> CREATE EXTERNAL TABLE `test_indexr`(
  `date` int,
  `user_id` bigint,
  `user_name` string,
  `cost` double)
PARTITIONED by (dt string)
ROW FORMAT SERDE                                                           
  'io.indexr.hive.IndexRSerde'
STORED AS 
  INPUTFORMAT 'io.indexr.hive.IndexRInputFormat'
  OUTPUTFORMAT 'io.indexr.hive.IndexROutputFormat'
LOCATION
  '/indexr/segment/test_indexr'
;

hive (default)> insert overwrite table test_indexr partition (dt='20160701') values (20160701, 11, 'flow', 1.3);
hive (default)> select * from test_indexr limit 10;
hive (default)> select user_id, user_name, sum(cost) from test_indexr group by user_id, user_name limit 10;
```

上面的命令创建了 `test_indexr` 表，然后插入了一条记录，并且尝试查询内容。它是标准的 Hive 语法，使用了 IndexR 的文件格式。有几个注意事项：

* 一般情况下请使用 EXTERNAL 表。
* 填写正确的 FORMAT 以及 SERDE：

	```
	ROW FORMAT SERDE                                                           
  	   'io.indexr.hive.IndexRSerde'
	STORED AS 
	  INPUTFORMAT 'io.indexr.hive.IndexRInputFormat'
	  OUTPUTFORMAT 'io.indexr.hive.IndexROutputFormat'
	  
	```
* 填写正确的表存储路径。存储路径目前是固定的，格式为 `/indexr/segment/<table_name>`
* Hive表必须要有且只有一个分区字段，一般为日期，方便管理（本例为 `dt` 即按天分割）。分区字段在 IndexR 的实时数据到历史数据的转换中是必须的。

上面创建的 `test_indexr` 表就是一个普通的 Hive 表，你可以对它进行任何 Hive 操作，包括导入数据，删除某一分区的数据等等，注意在数据操作完成之后，IndexR 不一定会注意到数据的修改，需要通知 IndexR 数据的更改，使用 `indexr-tool` 工具：

```
[flow@localhost:~]$ cd /usr/local/indexr-tool
[flow@localhost:/usr/local/indexr-tool]$ bin/tools.sh -cmd notifysu -t test_indexr
```

#### 2.1.2 创建 IndexR 表。

使用 `indexr-tool` 工具

```
[flow@localhost:~]$ cd /usr/local/indexr-tool
[flow@localhost:/usr/local/indexr-tool]$ bin/tools.sh -cmd settb -t test_indexr -c test_inexr_schema.json
```

查看命令 `bin/tools.sh -h` 可以看到各个参数的意义。`test_inexr_schema.json` 的文件内容如下：

```
{
    "schema":{
        "columns":
        [
            {"name": "date", "dataType": "int"},
            {"name": "user_id", "dataType": "long"},
            {"name": "user_name", "dataType": "string"},
            {"name": "cost", "dataType": "double"}
        ]
    },
    "realtime":{
        "dims": [
            "date",
            "user_id",
            "user_name"
        ],
        "metrics": [
            {"name": "cost", "agg": "sum"}
        ],
        "name.alias": {
        	"user_id": "ui"
        },
        "grouping": true,
        "save.period.minutes": 20,
        "upload.period.minutes": 60,
        "max.row.memory": 500000,
        "max.row.realtime": 10000000,
        "ingest": true,
        "compress": true,
        "fetcher": {
            "type": "kafka-0.8",
            "topic": "test_topic",
            "properties": {
                "zookeeper.connect": "localhost:2181",
                "zookeeper.connection.timeout.ms": "15000",
                "zookeeper.session.timeout.ms": "40000",
                "zookeeper.sync.time.ms": "5000",
                "fetch.message.max.bytes": "1048586",
                "auto.offset.reset": "largest",
                "auto.commit.enable": "true",
                "auto.commit.interval.ms": "5000",
                "group.id": "test_indexr"
            }
        }
    }
}

```

其中 "schema" 是必填，"realtime" 在需要实时导入数据的时候才需要。

* `schema` - 表定义，必填
* `schema.columns` - 表字段定义
* `realtime` - [选填]实时入库定义，在需要实时导入数据的表可以填写。
* `realtime.dims` - 维度定义。所有这些字段相同的记录有可能会被聚合成一条记录，可以有效压缩
* `realtime.metrics` - 指标定义。被聚合的字段以及聚合的方式，目前 `agg` 支持 `sum`, `last`, `first`, `min`, `max`
* `realtime.name.alias` - 字段入库时可以使用别名。
* `realtime.grouping` - 是否聚合数据，如果不聚合，则不需要 `realtime.metrics`，一般为 `true`
* `realtime.save.period.minutes` - 实时数据在内存停留的时间，一般设置 20 分钟。
* `realtime.upload.period.minutes` - 实时数据上传到 HDFS 的时间间隔，一般设置 60 分钟。
* `realtime.max.row.memory` - 一个实时segment最大放置在内存中的数据量，用于防止爆内存，超过这数量，内存中的数据会刷盘。
* `realtime.max.row.realtime` - 一个实时segment最大的数据量，超过这个数据量，数据会被上传。
* `realtime.ingest` - 是否实时导入，一般为 true。
* `realtime.compress` - 是否压缩，一般为 true。
* `realtime.fetcher` - 实时导入数据源。示例为 kafka 0.8。注意 `group.id`，不同表之间不要重复了。
	
执行完命令之后，可以去 Drill 查看表定义是否成功。进入 Drill console，如 

```
[flow@localhost:~]$ cd /usr/local/drill
[flow@localhost:/usr/local/drill]$ bin/drill-conf
0: jdbc:drill:> use indexr;
0: jdbc:drill:> show tables;
0: jdbc:drill:> select * from test_indexr limit 10;
```

这个时候，可以查询到之前从 Hive 导入的数据，但是实时导入还没有生效。

#### 2.1.3 配置实时导入 

* 把一些数据导入 kafka 的 `test_topic`。数据格式以 json 形式，并且一行可以写多条记录，逗号分隔：

```
{"date": 20160802, "user_id": 111, "user_name": "Alice", "cost": 2.3}, {"date": 20160804, "user_id": 222, "user_name": "Bob", "cost": 1.9}
{"date": 20160809, "user_id": 333, "user_name": "Mary", "cost": 4.5}
...

```

* 配置表的实时导入节点

首先需要了解当前有几个 IndexR 的可用节点

```
[flow@localhost:/usr/local/indexr-tool]$ bin/tools.sh -cmd listnode
hostA
hostB
hostC
hostD

```

然后选定一些节点做为表 `test_indexr` 的实时节点，如 `hostA,hostC`:

```
[flow@localhost:/usr/local/indexr-tool]$ bin/tools.sh -cmd addrtt -t test_indexr -host hostA,hostC
OK
[flow@localhost:/usr/local/indexr-tool]$ bin/tools.sh -cmd rttnode -t test_indexr
hostC
hostA
```

之后，被选定的节点将会开始从配置的 kafka 的 `test_topic` 导入数据到表 `test_indexr` 中。

可以查看 `bin/tools.sh -h` 了解更多的操作命令。

* 实时数据导入历史数据

实时数据从 kafka 导入到 IndexR 的实时节点之后，会被分段（segment）周期性的上传到 HDFS 的 `/indexr/segment/test_indexr/rt` 目录。实时数据只要从 kafka 读出并写入 IndexR 的实时节点，就立刻可以在 Drill 中被查询到。但是并不能被 Hive 查询，因为 `rt` 目录不是一个合法的分区目录，所有要配置一个周期性任务，比如每天凌晨把数据从 `rt` 目录转移进相应的分区目录。如在导入实时数据之后，`test_indexr` 表的目录结构可能是：

```
[flow@localhost:/usr/local/hadoop]$ bin/hdfs dfs -ls -R /indexr/segment/test_indexr
-rw-r--r--   2 dc supergroup          0 2016-08-30 18:31 /indexr/segment/test_indexr/__UPDATE__
drwxr-xr-x   - dc supergroup          0 2016-08-30 18:29 /indexr/segment/test_indexr/dt=20160701
-rw-r--r--   2 dc supergroup        938 2016-08-30 18:29 /indexr/segment/test_indexr/dt=20160701/000000_0
-rw-r--r--   2 dc supergroup          0 2016-08-30 18:25 /indexr/segment/test_indexr/rt/
-rw-r--r--   2 dc supergroup      58661 2016-08-30 18:25 /indexr/segment/test_indexr/rt/rtsg.201608301725.e06ca84e-1d65-4f37-a799-b406d01dd10e.seg
```

执行

```
[flow@localhost:/usr/local/indexr-tool]$ bin/rt2his.sh -hivecnn "jdbc:hive2://localhost:10000/default;auth=noSasl" -hiveuser dc -hivepwd dc -table test_indexr -segpc \`date\`
```

`rt2his.sh` 的执行流程是：

* 首先遍历一遍所有在 `rt` 表中的数据，找出 `DISTINCT(<segment_partition_column>)`，即找出 `rt` 文件夹中所有的分区，比如包含有 `20160801`,`20160802` 这两个分区的数据，伪代码：

```
select distinct(date) from rt_tmp_table;
```

* 然后对每个分区分别导入数据，伪代码:

```
insert into table `test_indexr` partition (dt = '20160801') select `date`, `user_id`, `user_name`, `cost` from rt_tmp_table where `date` = 20160801;
insert into table `test_indexr` partition (dt = '20160802') select `date`, `user_id`, `user_name`, `cost` from rt_tmp_table where `date` = 20160802;

```

可以注意到 `date` 和 Hive 表的分区字段 `dt` 的意义必须一样的，只有这样，数据才可以分别被整理进对应的分区。这也是 Hive 表必须要有一个分区字段的原因。

再查看表的数据目录结构，可以看到数据已经从 `rt` 目录转移入它对应的分区，这时候可以从 Hive 中查询到，并且可以方便的对分区的数据进行管理了：

```
[flow@localhost:/usr/local/hadoop]$ bin/hdfs dfs -ls -R /indexr/segment/test_indexr
-rw-r--r--   2 dc supergroup          0 2016-08-30 18:31 /indexr/segment/test_indexr/__UPDATE__
drwxr-xr-x   - dc supergroup          0 2016-08-30 18:29 /indexr/segment/test_indexr/dt=20160701
-rw-r--r--   2 dc supergroup        938 2016-08-30 18:29 /indexr/segment/test_indexr/dt=20160701/000000_0
-rw-r--r--   2 dc supergroup       2312 2016-08-30 18:29 /indexr/segment/test_indexr/dt=20160801/000000_0
-rw-r--r--   2 dc supergroup       1463 2016-08-30 18:29 /indexr/segment/test_indexr/dt=20160802/000000_0
-rw-r--r--   2 dc supergroup          0 2016-08-30 18:25 /indexr/segment/test_indexr/rt/
```

执行 `rt2his` 命令不需要手动通知数据更新，`rt2his` 已经帮我们完成了。

### 2.2 删除表

需要两个步骤，删除在  IndexR 中的表定义和删除 Hive 表

* 删除 IndexR 表

删除 IndexR 表不会删除任何数据，实时导入进程会停止，并且之后把表重新加回来，数据不会丢失。
	
```
[flow@localhost:/usr/local/indexr-tool]$ bin/tools.sh -cmd rmtable -t test_indexr
```
* 删除 Hive 表

删除 Hive 表有可能会照成数据丢失。

```
hive (default)> drop table test_indexr;
```


### 2.3 更新表结构

更新表结构需要使用 `upcol.sh` 工具。更新流程如下：

* **停止执行 rt2his 等会影响数据的脚本** - 防止更新过程中不可预料的问题。
* **更新 indexr 表定义** - 修改 schema.json，然后执行命令，如 `bin/tools.sh -cmd settb -t test_indexr -c test_inexr_schema.json`。本操作会更新 indexr 的表定义，可以通过 Drill cli 执行 `describe test_indexr` 参看是否更新成功。注意这时候只是表定义更新了，数据并没有更新，还不能执行相关更新列的查询，但是不影响没有变动的列的查询。
* **更新 hive 表定义** - drop 掉 Hive 的表定义，重新建一个新的表。在 EXTERNAL 定义下 drop Hive 表不会删掉数据。
* **等待旧的实时 segment 上传** - 如果有实时表，则要等待本地的 segment 全部上传到 hdfs 之后才能进行下一步操作，通常等待 20分钟 是个好选择。如果没有实时表则不需要等待。
* **更新历史 segment** - 执行以下命令之一，更新历史数据
	* **添加列** - 
	
		```
		bin/upcol.sh -add -t test_indexr -col '[{"name": "new_col1", "dataType": "long", "value": "100"}]'
		``` 
		这里 `value` 可以是某一个值，如 100；也可以是一个 SQL，如 `if((a > b), a + b, a / c)`。可以同时加多个列。
	
	* **删除列** - 
		
		```
		bin/upcol.sh -del -t test_indexr -col old_col1,old_col2
		```
		
	* **修改列** - 
		
		```
		bin/upcol.sh -alt -t test_indexr -col '[{"name": "col1", "dataType": "string", "value": "cast(col1, string)"}]'
		```
		同样，这里`value` 可以是某一个值，也可以是一个 SQL。还可以支持修改数据类型。可以同时修改多个列，不存在的列则跳过。

* **验证** - 在 Drill cli 中执行相关更新列的查询，查看是否正常。

## 3 更新数据内容

目前可以通过操作 Hive 表达到更新离线数据的目的。

## 4 查询
	
```
[flow@localhost:~]$ cd /usr/local/drill
[flow@localhost:/usr/local/drill]$ bin/drill-conf
0: jdbc:drill:> use indexr;

```
查询是标准的 SQL，参考 https://drill.apache.org/docs/

注意：目前 IndexR 通过 Drill 查询还不支持 join 的索引，特别慢和耗费资源，不要使用！ 但是不影响 Hive 表的查询。
