package org.apache.flink.table.examples.java.basics;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.examples.java.entity.ClickHouseSqlEntity;
import org.apache.flink.table.examples.java.entity.LineParseFlatMap;
import org.apache.flink.table.examples.java.util.ClickHouseUtil;
import org.apache.flink.table.examples.java.util.FlinkUtils;
import org.apache.flink.types.Row;


public class MyJdbcSinkTest {

    public static void main(String[] args) {
        /*args = new String[]{
                "-bootstrap.servers", "node1:9092",
                "-topics", "test",
                "-group.id", "test_g1",
                "-ck.url", "node1:8123",
                "-ck.username", "",
                "-ck.password", "",
        };*/
        /*args = new String[]{
                "-bootstrap.servers", "10.203.146.135:9092,10.203.146.136:9092,10.203.146.153:9092,10.203.146.154:9092,10.203.146.171:9092,10.203.146.199:9092,10.203.146.217:9092,10.203.146.218:9092,10.203.146.235:9092,10.203.146.236:9092",
                "-topics", "logsget_360loan",
                "-group.id", "beijing_jinrong_g1"
        };*/

        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStreamSource<String> sourceStream = env.socketTextStream("node1", 8989);
		env.setParallelism(1);

		FlinkUtils.initEnv(env, parameterTool);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        try {
            //过滤需要的内容，解密、解析为json
			SingleOutputStreamOperator<Row> lines = sourceStream.flatMap(new LineParseFlatMap(), getRowTypeInfo()).uid("LineParseFlatMap").name("LineParseFlatMap");

			//source table
            Table sourceTable = tableEnv.fromDataStream(lines);

            //create sink table
            String vmTable = "FlinkSinkTable";


			ClickHouseSqlEntity clickHouseSqlEntity = new ClickHouseSqlEntity();
			clickHouseSqlEntity.setDbName("test_local");
			clickHouseSqlEntity.setUsername("ck_dev");
			clickHouseSqlEntity.setPassword("HQg5SpsCkuaqj30x");
			clickHouseSqlEntity.setJdbcVmTableName(vmTable);
			clickHouseSqlEntity.setTableName("wh_test");

			tableEnv.executeSql(ClickHouseUtil.genarateJdbcCreateTableSql(clickHouseSqlEntity));

            //insert
            tableEnv.executeSql("insert into "+vmTable+" select * from "+sourceTable);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

	public static RowTypeInfo getRowTypeInfo() {
		TypeInformation[] types = new TypeInformation[2]; // 7个字段
		String[] fieldNames = new String[2];

		types[0] = BasicTypeInfo.LONG_TYPE_INFO;
		types[1] = BasicTypeInfo.STRING_TYPE_INFO;

		//userNo, itemid, m2, eventKey, eventTime, os, appkey
		fieldNames[0] = "id";
		fieldNames[1] = "cust_no";

		return new RowTypeInfo(types, fieldNames);
	}
}
