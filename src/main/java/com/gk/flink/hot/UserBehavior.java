package com.gk.flink.hot;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * @Description: 用户行为
@Author: GK
 * @Date: 2019/12/26
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class UserBehavior {

    /**
     * 用户 ID
     */
    private long userId;

    /**
     * 商品 ID
     */
    private long itemId;

    /**
     * 商品类目 ID
     */
    private int categoryId;

    /**
     * 用户行为，包括("pv", "buy", "cart", "fav")
     */
    private String behavior;

    /**
     * 用户行为发生的时间戳
     */
    private long timestamp;

}
