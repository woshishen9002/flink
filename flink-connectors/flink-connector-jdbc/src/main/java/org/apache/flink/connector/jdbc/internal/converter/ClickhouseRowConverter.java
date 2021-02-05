package org.apache.flink.connector.jdbc.internal.converter;

import org.apache.flink.table.types.logical.RowType;

public class ClickhouseRowConverter extends AbstractJdbcRowConverter {
  private static final long serialVersionUID = 1L;
  
  public String converterName() {
    return "Clickhouse";
  }
  
  public ClickhouseRowConverter(RowType rowType) {
    super(rowType);
  }
}
