package com.atguigu.gmall0624.publisher.service.impl;

import com.atguigu.gmall0624.publisher.mapper.DauMapper;
import com.atguigu.gmall0624.publisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    DauMapper dauMapper;

    @Override
    public Long getDauTotal(String date) {
        Long dauTotal = dauMapper.getDauTotal(date);
        return dauTotal;
    }

    @Override
    public Map getDauHour(String date) {
        List<Map> dauHourMapList = dauMapper.getDauHour(date);  // [{"loghour":9,"ct",435},"loghour":11,"ct",790}]
        Map dauHourMap=new HashMap();//{"9":435, "11":790}

        for (Map map : dauHourMapList) {
            dauHourMap.put( map.get("loghour"),map.get("ct"));
        }

        return dauHourMap;
    }
}
