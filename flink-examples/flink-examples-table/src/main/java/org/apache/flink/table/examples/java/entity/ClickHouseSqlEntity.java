package org.apache.flink.table.examples.java.entity;

import java.util.Map;

/**
 * 描述clickhouse 连接信息，建表语句等内容的对象
 */
public class ClickHouseSqlEntity {

    private String[] hosts;
    private String username;
    private String password;
    private String dbName;
    private String tableName;
    private Integer flushMaxRows = 10000;
    private String flushInterval = "10s";

    private String jdbcCreateTableSql;
    private String jdbcVmTableName;
    private Map<String, String> jdbcFields;

    public String[] getHosts() {
        return hosts;
    }

    public void setHosts(String[] hosts) {
        this.hosts = hosts;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Integer getFlushMaxRows() {
        return flushMaxRows;
    }

    public void setFlushMaxRows(Integer flushMaxRows) {
        this.flushMaxRows = flushMaxRows;
    }

    public String getFlushInterval() {
        return flushInterval;
    }

    public void setFlushInterval(String flushInterval) {
        this.flushInterval = flushInterval;
    }

    public String getJdbcCreateTableSql() {
        return jdbcCreateTableSql;
    }

    public void setJdbcCreateTableSql(String jdbcCreateTableSql) {
        this.jdbcCreateTableSql = jdbcCreateTableSql;
    }

    public String getJdbcVmTableName() {
        return jdbcVmTableName;
    }

    public void setJdbcVmTableName(String jdbcVmTableName) {
        this.jdbcVmTableName = jdbcVmTableName;
    }

    public Map<String, String> getJdbcFields() {
        return jdbcFields;
    }

    public void setJdbcFields(Map<String, String> jdbcFields) {
        this.jdbcFields = jdbcFields;
    }
}
