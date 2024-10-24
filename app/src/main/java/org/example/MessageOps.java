package org.example;

import java.nio.ByteBuffer;
import org.json.JSONObject;


public class MessageOps{

  // TODO initialize message as ByteBuffer
  //public static ByteBuffer sampleMessageBb = "";

  //
  public static String sampleMessageStr = "{\"date\":\"2020-06-07\",\"sym\":\"DUMMY\",    \"time\":\"22:58\",    \"lowAskSize\":20,    \"highAskSize\":790,    \"lowBidPrice\":43.13057,    \"highBidPrice\":44.95833,    \"lowBidSize\":60,    \"highBidSize\":770,    \"lowTradePrice\":43.51274,    \"highTradePrice\":45.41246,    \"lowTradeSize\":0,    \"highTradeSize\":480,    \"lowAskPrice\":43.67592,    \"highAskPrice\":45.86658, \"vwap\":238.0331}";

  public static void main(String[] args) {
    JSONObject converted = new JSONObject(sampleMessageStr);
    System.out.println(sampleMessageStr);
    System.out.println(converted);
  }
}