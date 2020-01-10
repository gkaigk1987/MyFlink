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
public class LoginWarning {

    private Integer userId;

    private String userIp;

    private Long firstFailTime;

    private Long secondFailTime;

    private String msg;

}
