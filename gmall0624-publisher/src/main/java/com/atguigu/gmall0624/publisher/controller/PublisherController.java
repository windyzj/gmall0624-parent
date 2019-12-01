package com.atguigu.gmall0624.publisher.controller;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmall0624.publisher.service.PublisherService;
import org.apache.commons.lang.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@RestController
public class PublisherController {

    @Autowired
    PublisherService publisherService;

    /**
     * 各种总值
     * @param date
     * @return
     */
    @GetMapping("realtime-total")
    public String getRealtimeTotal(@RequestParam("date") String date){
        Long dauTotal = publisherService.getDauTotal(date);
        List<Map> totalList=new ArrayList<>();

        Map dauMap=new HashMap();
        dauMap.put("id","dau");
        dauMap.put("name","新增日活");
        dauMap.put("value", dauTotal );
        totalList.add(dauMap);

        Map newMidMap=new HashMap();
        newMidMap.put("id","new_mid");
        newMidMap.put("name","新增设备");
        newMidMap.put("value", 233 );
        totalList.add(newMidMap);

        Map orderAmountMap=new HashMap();
        Double orderAmount = publisherService.getOrderAmount(date);
        orderAmountMap.put("id","order_amount");
        orderAmountMap.put("name","新增交易额");
        orderAmountMap.put("value", orderAmount );
        totalList.add(orderAmountMap);

        return JSON.toJSONString(totalList);
    }

    @GetMapping("realtime-hour")
    public String getRealtimeHour(@RequestParam("id") String id , @RequestParam("date") String td ){


        if(id.equals("dau")){  //跟id不同  取不同的分时图
            Map dauHourMapTD = publisherService.getDauHour(td);
            String yd = getYd(td);
            Map dauHourMapYD = publisherService.getDauHour(yd);

            Map hourMap = new HashMap();
            hourMap.put("today",dauHourMapTD);
            hourMap.put("yesterday",dauHourMapYD);

            return JSON.toJSONString(hourMap);
        }else if(id.equals("order_amount")){
            Map orderAmountHourMapTD = publisherService.getOrderAmountHourMap(td);  //当天
            String yd = getYd(td);
            Map orderAmountHourMapYD = publisherService.getOrderAmountHourMap(yd);  //前一天

            Map hourMap = new HashMap();
            hourMap.put("today",orderAmountHourMapTD);
            hourMap.put("yesterday",orderAmountHourMapYD);

            return JSON.toJSONString(hourMap);
        }else{
            return  null;
        }


    }

    private String getYd(String td){
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        try {
            Date today = simpleDateFormat.parse(td);
            Date yesterday = DateUtils.addDays(today, -1);
            return simpleDateFormat.format(yesterday);

        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;

    }
}
