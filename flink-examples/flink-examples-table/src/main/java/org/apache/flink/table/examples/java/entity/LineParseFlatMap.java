package org.apache.flink.table.examples.java.entity;


import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

public class LineParseFlatMap extends RichFlatMapFunction<String, Row> {

    @Override
    public void flatMap(String value, Collector<Row> out) {
        try {
            String[] fieldArray = value.split(" ");
            for(String field : fieldArray){
				String[] split = field.split("#");
				Row row = Row.of(Long.valueOf(split[0]), split[1]);
				out.collect(row);
			}
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
