#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Date          : 2024-07-11
# @Author        : zeonto
# @File          : AnalysisBinlogTool.py
# @Description   : 解析 MySQL binlog 文件，统计 CURD 相关信息
# @Version       : 1.0.0
# @Last Modified : 2024-07-12 17:39:14
# @License       : Apache-2.0 License
# 
# @ChangeLog:
# - 2024-07-12 17:36  增加判断限制读取原始binlog文件
# - 2024-07-11 14:36  创建文件
#

import os
import sys
import re
import json
import time

# 全局变量
GL_START_TIME = time.time()
GL_DATABASE_NAME = ""
GL_CURD_STAT_DICT = {
    "INSERT": 0,
    "UPDATE": 0,
    "SELECT": 0,
    "DELETE": 0
}
GL_TOTAL_CURD_STAT = GL_CURD_STAT_DICT.copy()
GL_TABLE_CURD_STAT = {}

def read_file(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        while True:
            line = f.readline()
            if not line:
                break
            yield line.strip()

def is_binlog_file(file):
    with open(file, 'rb') as f:
        bytes_data = f.read(4)
        data = bytes_data.hex()
        return data == 'fe62696e'

def dbname_from_use_statement(line):
    matches = re.match(r"^USE\s+`(\w+)`", line, re.IGNORECASE)
    if matches:
        return matches.group(1)
    return None

def set_database_name(name):
    global GL_DATABASE_NAME
    GL_DATABASE_NAME = name

def get_table_name(table_name):
    if GL_DATABASE_NAME:
        return "%s.%s" % (GL_DATABASE_NAME, table_name)
    return table_name

def curd_statement(line):
    matches = re.match(r"^INSERT|^UPDATE|^SELECT|^DELETE", line, re.IGNORECASE)
    if matches:
        return matches.group()
    return None

def stat_total_curd_info(curd_type):
    global GL_TOTAL_CURD_STAT
    GL_TOTAL_CURD_STAT[curd_type] += 1

def stat_table_curd_info(table_name, curd_type):
    global GL_TABLE_CURD_STAT
    if table_name not in GL_TABLE_CURD_STAT:
        GL_TABLE_CURD_STAT[table_name] = GL_CURD_STAT_DICT.copy()
    GL_TABLE_CURD_STAT[table_name][curd_type] += 1

def parse_curd_statement(curd_type, line):
    curd_rule = {
        "INSERT": r"^INSERT\s+INTO\s+`(?P<table_name>\w+)`\s*\((.+?)\)\s*VALUES\s*\((.+?)\)",
        "UPDATE": r"^UPDATE\s+`(?P<table_name>\w+)`\s+SET\s+(.+?)\s+WHERE\s+(.+?)$",
        "SELECT": r"^SELECT\s+(.+?)\s+FROM\s+`(?P<table_name>\w+)`\s+WHERE\s+(.+?)$",
        "DELETE": r"^DELETE\s+FROM\s+`(?P<table_name>\w+)`\s+WHERE\s+(.+?)$"
    }
    if curd_type in curd_rule:
        stat_total_curd_info(curd_type)
        matches = re.match(curd_rule[curd_type], line, re.IGNORECASE)
        if matches and "table_name" in matches.groupdict():
            table_name = get_table_name(matches.group("table_name"))
            stat_table_curd_info(table_name, curd_type)

def main():
    # 检查参数
    if len(sys.argv) < 2:
        print("Usage: python %s bin.sql 10000\n参数1: binlog 文件路径；\n参数2: 读取文件行数（可选，默认不限制）" % os.path.basename(__file__))
        exit(1)
    # 检查文件是否存在
    file_path = sys.argv[1]
    if not os.path.exists(file_path):
        print("File not exists.")
        exit(1)
    if is_binlog_file(file_path):
        print("This is a binlog file, please use mysqlbinlog tool to export result first.")
        exit(1)
    # 设置读取行数限制
    line_limit = int(sys.argv[2]) if len(sys.argv) > 2 else 0
    # 解析 binlog 文件
    line_count = 0
    for line in read_file(file_path):
        line_count += 1
        database_name = dbname_from_use_statement(line)
        if database_name:
            set_database_name(database_name)
        curd_type = curd_statement(line)
        if curd_type:
            parse_curd_statement(curd_type, line)
        if line_limit > 0 and line_count >= line_limit:
            break

    # 输出日志总的 CURD 统计信息
    summary_stat = {
        "BINLOG_LINE": line_count,
        "TABLE_COUNT": len(GL_TABLE_CURD_STAT),
        "USE_TIME": "%.4fs" % (time.time() - GL_START_TIME),
        "TOTAL_CURD_STAT": GL_TOTAL_CURD_STAT,
    }
    
    # 输出每个表的 CURD 统计信息
    # 依次按 UPDATE、INSERT、DELETE、SELECT 的值进行降序排序字典
    sorted_table_curd_stat = dict(sorted(GL_TABLE_CURD_STAT.items(), key=lambda x: (x[1]['UPDATE'], x[1]['INSERT'], x[1]['DELETE'], x[1]['SELECT']), reverse=True))
    print("Summary Stats: ", json.dumps(summary_stat))
    print("Table CURD Stats: \n", json.dumps(sorted_table_curd_stat, indent=4))

if __name__ == '__main__':
    main()