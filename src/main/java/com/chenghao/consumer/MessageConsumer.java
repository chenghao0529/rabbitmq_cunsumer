package com.chenghao.consumer;

import com.chenghao.domain.Person;
import com.rabbitmq.client.Channel;
import org.springframework.amqp.rabbit.annotation.Exchange;
import org.springframework.amqp.rabbit.annotation.Queue;
import org.springframework.amqp.rabbit.annotation.QueueBinding;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Map;

/**
 * Created by 程昊 on 2019/8/19.
 */
@Component
public class MessageConsumer {

    //@RabbitListener注解用于声明式定义消息接受的队列与exhcange绑定的信息
    @RabbitListener(
            bindings = @QueueBinding(
                    value = @Queue(value="rabbitmq-consumer" , durable="true"),
                    exchange = @Exchange(value = "rabbitmq-test") ,
                    key = "chenghao"
            )
    )
    //@Payload 代表运行时将消息反序列化后注入到后面的参数中
    public void handleMessage(@Payload Person person , Channel channel ,
                              @Headers Map<String,Object> headers) {
        System.out.println(person.getUsername() + "-" + person.getAge());
        //所有消息处理后必须进行消息的ack，channel.basicAck()
        Long tag = (Long)headers.get(AmqpHeaders.DELIVERY_TAG);
        try {
            //false表示不批量接收
            channel.basicAck(tag , false);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
