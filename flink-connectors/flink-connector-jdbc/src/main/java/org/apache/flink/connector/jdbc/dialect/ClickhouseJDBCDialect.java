package org.apache.flink.connector.jdbc.dialect;

import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.flink.connector.jdbc.internal.converter.ClickhouseRowConverter;
import org.apache.flink.connector.jdbc.internal.converter.JdbcRowConverter;
import org.apache.flink.table.types.logical.RowType;

public class ClickhouseJDBCDialect implements JdbcDialect {
  private static final long serialVersionUID = 1L;
  
  private static final int MAX_TIMESTAMP_PRECISION = 6;
  
  private static final int MIN_TIMESTAMP_PRECISION = 1;
  
  private static final int MAX_DECIMAL_PRECISION = 65;
  
  private static final int MIN_DECIMAL_PRECISION = 1;
  
  public JdbcRowConverter getRowConverter(RowType rowType) {
    return (JdbcRowConverter)new ClickhouseRowConverter(rowType);
  }
  
  public boolean canHandle(String url) {
    return url.startsWith("jdbc:clickhouse:");
  }
  
  public Optional<String> defaultDriverName() {
    return Optional.of("ru.yandex.clickhouse.ClickHouseDriver");
  }
  
  public String quoteIdentifier(String identifier) {
    return "`" + identifier + "`";
  }
  
  public Optional<String> getUpsertStatement(String tableName, String[] fieldNames, String[] uniqueKeyFields) {
    String columns = Arrays.<CharSequence>stream((CharSequence[])fieldNames).collect(Collectors.joining(", "));
    String placeholders = Arrays.<String>stream(fieldNames).map(f -> quoteIdentifier(f)).collect(Collectors.joining(", "));
    return Optional.of(getInsertIntoStatement(tableName, fieldNames));
  }
  
  public String dialectName() {
    return "Clickhouse";
  }
}
