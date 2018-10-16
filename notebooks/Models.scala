// Databricks notebook source
class SalesDataDim 
{
  var HK_SalesDateID :Int;
  var SalesDateID :Int;
  var CalendarDate: String;
  var Quarter: Int;
  var ModifiedDate:String;
}

class Earnings
{
  var OrderDate:String;
  var SalesDate:String;
  var HK_SalesDate_OrderDate :Int;
  var HK_SalesDate_SalesDate: Int;
  var HK_SalesDate_DeliveryDate: Int;
  var ModifiedDate:String;
  var HK_Partners_PID_Type:Int;
}