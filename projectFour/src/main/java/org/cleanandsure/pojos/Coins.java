package org.cleanandsure.pojos;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Date;

public class Coins {
  
  private int credit_card;
  private String name;
  private int cmc_rank;
  private String date_added;
  private String last_updated;
  private double market_cap;
  private double price;
  private double percent_change_1h;
  private double percent_change_24h;
  private double percent_change_7d;
  private double volume_24h;
  private String slug;
  private String symbol;
  private double total_supply;
  
  
  public int getCredit_card() {
    return credit_card;
  }
  
  public void setCredit_card(int credit_card) {
    this.credit_card = credit_card;
  }
  
  public String getName() {
    return name;
  }
  
  public void setName(String name) {
    this.name = name;
  }
  
  public int getCmc_rank() {
    return cmc_rank;
  }
  
  public void setCmc_rank(int cmc_rank) {
    this.cmc_rank = cmc_rank;
  }
  
  public String getDate_added() {
    return date_added;
  }
  
  public void setDate_added(String date_added) {
    this.date_added = date_added;
  }
  
  public String getLast_updated() {
    return last_updated;
  }
  
  public void setLast_updated(String last_updated) {
    this.last_updated = last_updated;
  }
  
  public double getMarket_cap() {
    return market_cap;
  }
  
  public void setMarket_cap(double market_cap) {
    this.market_cap = market_cap;
  }
  
  public double getPrice() {
    return price;
  }
  
  public void setPrice(double price) {
    this.price = price;
  }
  
  public double getPercent_change_1h() {
    return percent_change_1h;
  }
  
  public void setPercent_change_1h(double percent_change_1h) {
    this.percent_change_1h = percent_change_1h;
  }
  
  public double getPercent_change_24h() {
    return percent_change_24h;
  }
  
  public void setPercent_change_24h(double percent_change_24h) {
    this.percent_change_24h = percent_change_24h;
  }
  
  public double getPercent_change_7d() {
    return percent_change_7d;
  }
  
  public void setPercent_change_7d(double percent_change_7d) {
    this.percent_change_7d = percent_change_7d;
  }
  
  public double getVolume_24h() {
    return volume_24h;
  }
  
  public void setVolume_24h(double volume_24h) {
    this.volume_24h = volume_24h;
  }
  
  public String getSlug() {
    return slug;
  }
  
  public void setSlug(String slug) {
    this.slug = slug;
  }
  
  public String getSymbol() {
    return symbol;
  }
  
  public void setSymbol(String symbol) {
    this.symbol = symbol;
  }
  
  public double getTotal_supply() {
    return total_supply;
  }
  
  public void setTotal_supply(double total_supply) {
    this.total_supply = total_supply;
  }
}
