# AnalysisBinlogTool

MySQL binlog 操作分析统计

该文件是分析 MySQL binlog 文件的工具脚本，用于统计 binlog 文件中 CURD 语句的数量，以及每个表的 CURD 统计信息。

汇总 CURD 统计信息包括：
- BINLOG_LINE：binlog 文件的总行数
- TABLE_COUNT：binlog 文件中包含的表数量
- USE_TIME：binlog 文件解析耗时
- TOTAL_CURD_STAT：binlog 文件中 INSERT、UPDATE、DELETE、SELECT 语句的数量

单表 CURD 统计信息包括：
- 表名：表名
- INSERT：表中插入操作数
- UPDATE：表中更新操作数
- DELETE：表中删除操作数
- SELECT：表中查询操作数

## 使用方法

```
python analysis_binlog.py <binlog_file> <limit:可选>
```

- `<binlog_file>`：binlog 文件路径，必填项
- `<limit>`：读取文件行数限制，可选，默认值为 0，表示不限制读取行数

> __注意事项__：该脚本不能直接读取 MySQL 原始的 binlog 文件，需要使用 mysqlbinlog 命令导出。

## 输出示例

```
Summary Stats:  {"BINLOG_LINE": 1000, "TABLE_COUNT": 2, "USE_TIME": "0.0530s", "TOTAL_CURD_STAT": {"INSERT": 0, "UPDATE": 74, "SELECT": 0, "DELETE": 0}}
Table CURD Stats:
 {
    "test.table_1": {
        "INSERT": 0,
        "UPDATE": 73,
        "SELECT": 0,
        "DELETE": 0
    },
    "test.table_2": {
        "INSERT": 0,
        "UPDATE": 1,
        "SELECT": 0,
        "DELETE": 0
    }
}
```