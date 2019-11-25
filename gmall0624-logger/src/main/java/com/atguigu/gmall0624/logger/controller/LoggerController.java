package com.atguigu.gmall0624.logger.controller;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

@RestController //   @ResponseBody+controller
@Slf4j
public class LoggerController {

    @PostMapping("log")
    public  String log( @RequestParam("logString") String logString){
        System.out.println(logString);
        //补时间戳
        JSONObject jsonObject = JSON.parseObject(logString);
        jsonObject.put("ts",System.currentTimeMillis());
        //落盘日志文件
        String jsonString = jsonObject.toJSONString();
        log.info(jsonString);


        //  发送kafka
       return "ok";
    }
}
