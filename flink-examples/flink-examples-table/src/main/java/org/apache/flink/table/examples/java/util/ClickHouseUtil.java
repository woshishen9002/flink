package org.apache.flink.table.examples.java.util;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.examples.java.entity.ClickHouseSqlEntity;

public class ClickHouseUtil {

//    private static String[] analyzeHosts = {"10.160.162.33:8123",  "10.160.162.34:8123",  "10.160.162.51:8123",  "10.160.162.52:8123",  "10.160.174.14:8123",  "10.160.174.31:8123",    "10.160.174.49:8123",  "10.160.174.78:8123",  "10.160.174.96:8123",  "10.160.174.114:8123"};
    private static String[] analyzeHosts = {"10.160.162.33:8123", "10.160.174.78:8123"};

    public static String genarateJdbcCreateTableSql(ClickHouseSqlEntity clickHouseSqlEntity){

        String jdbcVmTableName = clickHouseSqlEntity.getJdbcVmTableName();
        //String[] hosts = clickHouseSqlEntity.getHosts();
        String dbName = clickHouseSqlEntity.getDbName();
        String username = clickHouseSqlEntity.getUsername();
        String password = clickHouseSqlEntity.getPassword();
        String tableName = clickHouseSqlEntity.getTableName();

        String flushInterval = clickHouseSqlEntity.getFlushInterval();
        Integer flushMaxRows = clickHouseSqlEntity.getFlushMaxRows();

		String hostList = String.join("#", analyzeHosts);
		//String hostList = analyzeHosts[0];


		return "CREATE TABLE "+jdbcVmTableName+" (\n" +
                "       `id` BIGINT, \n" +
                "       `cust_no` STRING \n" +
                ") WITH (\n" +
                "       'connector' = 'jdbc',\n" +
                "       'url' = 'jdbc:clickhouse://["+hostList+"]/"+dbName+"',\n" +
                "       'driver' = 'ru.yandex.clickhouse.ClickHouseDriver',\n" +
                "       'username' = '"+username+"',\n" +  //
                "       'password' = '"+password+"',\n" + //HQg5SpsCkuaqj30x
                "       'sink.buffer-flush.max-rows' = '"+flushMaxRows+"',\n" +
                "       'sink.buffer-flush.interval' = '"+flushInterval+"',\n" +
                //"       'sink.per-ip-max-toal-count' = '3',\n" +  //wh add param
                "       'table-name' = '"+tableName+"'\n" +
                ")";
    }



}
