package com.atguigu.gmall0624.publisher.service;

import java.util.List;
import java.util.Map;

public interface PublisherService {

    public Long getDauTotal(String date);

    public Map getDauHour(String date);

    //求 单日交易额
    public  Double  getOrderAmount(String date);

    //求 单日分时交易额
    public  Map  getOrderAmountHourMap(String date);

    // 根据 日期、关键词 、 页码、单页行数 进行查询
    public  Map getSaleDetail(String dt,String keyword ,int pageNo,int pageSize);


}
