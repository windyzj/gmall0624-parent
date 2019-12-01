package com.atguigu.gmall0624.publisher.mapper;

import java.util.List;
import java.util.Map;

public interface DauMapper {

    //求日活总数
    public  Long  getDauTotal(String date);

    // 求 日活的分时数
    public List<Map> getDauHour(String date);




}
