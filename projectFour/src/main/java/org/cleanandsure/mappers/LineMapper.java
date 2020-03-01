package org.cleanandsure.mappers;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;

import java.util.Arrays;
import java.util.Iterator;

/*El input será un Row(cada linea del dataset) y la salida sera una lista de palabras (String)*/
public class LineMapper implements FlatMapFunction<Row, String> {
  @Override
  public Iterator<String> call(Row row) throws Exception {
    return Arrays.asList(row.toString().split(" ")).iterator();
  }
}
