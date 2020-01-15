package com.gk.flink.marketanalysis;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Description: TODO
 * @Author: GK
 * @Date: 2020/1/15
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class BlackListWarning {

    private Long userId;

    private Long adId;

    private String msg;

}
