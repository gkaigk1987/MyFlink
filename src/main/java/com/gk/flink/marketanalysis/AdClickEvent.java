package com.gk.flink.marketanalysis;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Description: 用户点击广告行为
 * @Author: GK
 * @Date: 2020/1/15
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class AdClickEvent {
    private Long userId;

    private Long adId;

    private String province;

    private String city;

    private Long timeStamp;
}
