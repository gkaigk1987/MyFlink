package com.gk.flink.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Description: TODO
 * @Author: GK
 * @Date: 2020/1/10
 */
@NoArgsConstructor
@AllArgsConstructor
@Data
public class PvCount {
    private String id;

    private Integer count;

    private String dateTime;
}
