package com.chenghao.domain;

import lombok.Data;

import java.io.Serializable;

/**
 * Created by 程昊 on 2019/8/19.
 */
@Data
public class Person implements Serializable{
    private String username;
    private int age;

    public Person(String username, int age) {
        this.username = username;
        this.age = age;
    }
}
