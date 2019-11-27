package com.atguigu.gmall0624.publisher;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan("com.atguigu.gmall0624.publisher.mapper")
public class Gmall0624PublisherApplication {

    public static void main(String[] args) {
        SpringApplication.run(Gmall0624PublisherApplication.class, args);
    }

}
