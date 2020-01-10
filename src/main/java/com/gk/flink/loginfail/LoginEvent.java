package com.gk.flink.loginfail;

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
public class LoginEvent {

    private Integer userId;

    private String userIp;

    private String type;

    private Long eventTime;

}
