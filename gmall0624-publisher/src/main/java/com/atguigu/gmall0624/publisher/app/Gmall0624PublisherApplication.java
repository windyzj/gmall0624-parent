package com.atguigu.gmall0624.publisher.app;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@MapperScan("com.atguigu.gmall0624.publisher.mapper")
@ComponentScan("com.atguigu.gmall0624.publisher")
public class Gmall0624PublisherApplication {

    public static void main(String[] args) {
        SpringApplication.run(Gmall0624PublisherApplication.class, args);
    }

}
