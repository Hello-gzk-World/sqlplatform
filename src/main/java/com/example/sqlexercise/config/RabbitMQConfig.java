//package com.example.sqlexercise.config;
//
//import org.springframework.amqp.core.Binding;
//import org.springframework.amqp.core.BindingBuilder;
//import org.springframework.amqp.core.DirectExchange;
//import org.springframework.amqp.core.Queue;
//import org.springframework.context.annotation.Bean;
//import org.springframework.context.annotation.Configuration;
//
//import java.util.HashMap;
//import java.util.Map;
//
///**
// * @author shenyichen
// * @date 2023/1/18
// **/
//@Configuration
//public class RabbitMQConfig {
//    public static final String DirectQueue = "DirectQueue";
//    public static final String DirectExchange = "DirectExchange";
//    public static final String DirectRouting = "DirectRouting";
//
//    /**
//     * 队列
//     */
//    @Bean
//    public Queue directQueue() {
//        // durable:是否持久化,默认是false,持久化队列：会被存储在磁盘上，当消息代理重启时仍然存在，暂存队列：当前连接有效
//        // exclusive:默认也是false，只能被当前创建的连接使用，而且当连接关闭后队列即被删除。此参考优先级高于durable
//        // autoDelete:是否自动删除，当没有生产者或者消费者使用此队列，该队列会自动删除。
//        //   return new Queue("TestDirectQueue",true,true,false);
//        // 一般设置一下队列的持久化就好,其余两个就是默认false
//        // 消息过期 特殊的args
////        return new Queue(DirectQueue,true);
//        Map<String,Object> args  = new HashMap<>(1);
//        args.put("x-message-ttl",20000); // 过期时间 单位ms
//        return new Queue(DirectQueue,true, false, false, args);
//    }
//
//    /**
//     * Direct交换机
//     */
//    @Bean
//    DirectExchange directExchange() {
//        //  return new DirectExchange("TestDirectExchange",true,true);
//        return new DirectExchange(DirectExchange,true,false);
//    }
//
//    /**
//     * 交换机队列绑定，并设置用于匹配键：DirectRouting
//     * @return
//     */
//    @Bean
//    Binding bindingDirect() {
//        return BindingBuilder.bind(directQueue()).to(directExchange()).with(DirectRouting);
//    }
//}
