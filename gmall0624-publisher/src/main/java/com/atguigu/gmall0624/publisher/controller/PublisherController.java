package com.atguigu.gmall0624.publisher.controller;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmall0624.publisher.bean.Option;
import com.atguigu.gmall0624.publisher.bean.SaleResult;
import com.atguigu.gmall0624.publisher.bean.Stat;
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

    @GetMapping("sale_detail")
    public String getSaleDetail(@RequestParam("date") String date,@RequestParam("startpage")int pageNo, @RequestParam("size") int pageSize,@RequestParam("keyword") String keyword ){
        Map resultMap = publisherService.getSaleDetail(date, keyword, pageNo, pageSize);
        Long total =(Long)resultMap.get("total");
        List<Map> detail =( List<Map>)resultMap.get("detail");
        Map ageMap =( Map)resultMap.get("ageAgg");
        Map genderMap   =( Map)resultMap.get("genderAgg");

        Long age_20ct=0L;
        Long age20_30ct=0L;
        Long age30_ct=0L;
        for (Object o : ageMap.entrySet()) {
            Map.Entry entry = (Map.Entry) o;
            String ageStr = (String)entry.getKey();
            Long count = (Long)entry.getValue();
            Integer age = Integer.valueOf(ageStr);
            if(age>=30){
                age30_ct+=count;
            }else if(age<30&&age>=20){
                age20_30ct+=count;
            }else{
                age_20ct+=count;
            }

        }
        double age_20rt= Math.round(age_20ct*1000D/total)/10D;
        double age20_30rt=Math.round(age20_30ct*1000D/total)/10D;
        double age30_rt=Math.round(age30_ct*1000D/total)/10D;


        List<Option> ageOptionList=new ArrayList<>();
        ageOptionList.add( new Option("20岁以下",age_20rt)) ;
        ageOptionList.add( new Option("20岁到30岁",age20_30rt)) ;
        ageOptionList.add( new Option("30岁及以上",age30_rt)) ;

        Stat ageStat = new Stat("年龄占比", ageOptionList);
        long maleCt=genderMap.get("M")!=null?(Long)genderMap.get("M"):0 ;
        long femaleCt=genderMap.get("F")!=null?(Long)genderMap.get("F"):0 ;



        Double maleRt=Math.round(maleCt*1000D/total)/10D;
        Double femaleRt=Math.round(femaleCt*1000D/total)/10D;

        List<Option> genderOptionList=new ArrayList<>();
        genderOptionList.add( new Option("男",maleRt)) ;
        genderOptionList.add( new Option("女",femaleRt)) ;

        Stat genderStat = new Stat("性别占比", genderOptionList);

        List<Stat> statList=new ArrayList<>();
        statList.add(ageStat);
        statList.add(genderStat);

        SaleResult saleResult = new SaleResult(total,statList,detail);
        return   JSON.toJSONString(saleResult);
    }


}
