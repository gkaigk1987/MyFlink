package com.gk.flink.marketanalysis;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Description: 按照省份统计信息的输出类
 * @Author: GK
 * @Date: 2020/1/15
 */
@NoArgsConstructor
@AllArgsConstructor
@Data
public class CountByProvince {

    private String windowEnd;

    private String province;

    private Long count;

}
