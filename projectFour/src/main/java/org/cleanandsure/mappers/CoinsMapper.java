package org.cleanandsure.mappers;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.cleanandsure.pojos.Coins;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

public class CoinsMapper implements MapFunction<Row, Coins> {
  @Override
  public Coins call(Row value) throws Exception {
    Coins c = new Coins();
    
    c.setCredit_card(value.getAs("credit_card"));
    c.setName(value.getAs("name"));
    c.setCmc_rank(value.getAs("cmc_rank"));
    
    String dateAddedAsString = value.getAs("date_added").toString();
    if (dateAddedAsString != null){
//      2013-04-28T00:00:00.000Z
/*      SimpleDateFormat parser = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      c.setDate_added(parser.parse(dateAddedAsString));*/
      c.setDate_added(readTime(dateAddedAsString, "yyyy-MM-dd HH:mm:ss", null ));
    }
    
    String lastUpdateAsString = value.getAs("last_updated").toString();
    if (lastUpdateAsString != null){
/*      SimpleDateFormat parser = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");
      c.setLast_updated(parser.parse(lastUpdateAsString));*/

      c.setLast_updated(readTime(lastUpdateAsString, "yyyy-MM-dd HH:mm:ss.S", null ));
    }
    
    c.setMarket_cap(value.getAs("market_cap"));
    c.setPercent_change_1h(value.getAs("percent_change_1h"));
    c.setPercent_change_24h(value.getAs("percent_change_24h"));
    c.setPercent_change_7d(value.getAs("percent_change_7d"));
    c.setPrice(value.getAs("price"));
    c.setSlug(value.getAs("slug"));
    c.setSymbol(value.getAs("symbol"));
    c.setTotal_supply(value.getAs("total_supply"));
    c.setVolume_24h(value.getAs("volume_24h"));
    return c;
  }
  public static String readTime(String tsStr, String pattern, Instant defaultValue) throws ParseException {
    
    SimpleDateFormat sdf = new SimpleDateFormat(pattern);
    Instant instant = sdf.parse(tsStr).toInstant();
  
    Date myDate = Date.from(instant);
    SimpleDateFormat formatter = new SimpleDateFormat("E, dd MM yyyy HH:mm:ss", Locale.GERMANY);
    formatter.setTimeZone(TimeZone.getTimeZone("ECT"));
    String formattedDate = formatter.format(myDate);
    
//    ZonedDateTime instant2 = instant.atZone(ZoneId.of("Europe/Paris"));
    return formattedDate;
  }
  
/*  public static String getTime(Instant cl, String format, String prd){
  
  }*/
/*  public static String formatInstant(Instant ts, String pattern, String defaultValue) {
    if (ts == null || pattern == null)
      return defaultValue;
    DateTimeFormatter format = DateTimeFormatter.ofPattern(pattern).withZone(ZoneId.of("UTC"));
    return format.format(ts);
  }*/
  
}
