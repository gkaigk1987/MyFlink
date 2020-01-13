package com.gk.flink.loginfail;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * @Description: TODO
 * @Author: GK
 * @Date: 2020/1/10
 */
public class LoginProcessFunction extends KeyedProcessFunction<Integer, LoginEvent, LoginWarning> {

    private ListState<LoginEvent> loginState;

    private Integer timeDurable = 2;

    public LoginProcessFunction() {

    }

    public LoginProcessFunction(Integer timeDurable) {
        this.timeDurable = timeDurable;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
//        super.open(parameters);
        ListStateDescriptor<LoginEvent> loginStateDesc = new ListStateDescriptor<>("loginStateDesc", LoginEvent.class);
        loginState = getRuntimeContext().getListState(loginStateDesc);
    }

    @Override
    public void processElement(LoginEvent value, Context ctx, Collector<LoginWarning> out) throws Exception {
        String type = value.getType();
        if("fail".equalsIgnoreCase(type)) {
            Iterator<LoginEvent> iterator = loginState.get().iterator();
            if(iterator.hasNext()) {
                LoginEvent lastFail = iterator.next();
                if(value.getEventTime() <= lastFail.getEventTime() + timeDurable) {
                    out.collect(new LoginWarning(value.getUserId(),value.getUserIp(),lastFail.getEventTime(),value.getEventTime(),"该用户两秒内登录失败"));
                }
                // 清空状态中的值
                loginState.clear();
                // 将最新的登录失败事件保存在状态中
                loginState.add(value);
            }else {
                // 将最新的登录失败事件保存在状态中
                loginState.add(value);
            }
        }else {
            // 登录成功，清空状态中的信息
            loginState.clear();
        }
    }

}
