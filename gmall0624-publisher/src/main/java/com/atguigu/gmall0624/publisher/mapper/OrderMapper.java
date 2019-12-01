package com.atguigu.gmall0624.publisher.mapper;

import java.util.List;
import java.util.Map;

public interface OrderMapper {

    //求 单日交易额
    public  Double  selectOrderAmount(String date);

    //求 单日分时交易额
    public List<Map>  selectOrderAmountHour(String date);
}
