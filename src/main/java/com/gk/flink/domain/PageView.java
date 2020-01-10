package com.gk.flink.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Description: TODO
 * @Author: GK
 * @Date: 2020/1/10
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class PageView {

    private Integer id;

    private Long createTime;
}
