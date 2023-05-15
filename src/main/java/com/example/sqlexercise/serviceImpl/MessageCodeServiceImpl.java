package com.example.sqlexercise.serviceImpl;

import com.example.sqlexercise.constant.Constants;
import com.example.sqlexercise.service.MessageCodeService;
import com.example.sqlexercise.vo.ResponseVO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;


import javax.mail.Authenticator;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

@Service
public class MessageCodeServiceImpl implements MessageCodeService {

    private StringRedisTemplate stringRedisTemplate;

    @Autowired
    public MessageCodeServiceImpl(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    @Override
    public ResponseVO sendMessageCode(String email, String code) {
        Properties properties = new Properties();
        //设置发送邮件的基本参数
        properties.put("mail.transport.protocol", "smtp");      // 协议
        properties.put("mail.smtp.host", "smtp.qq.com");        // QQ邮件服务器地址
        properties.put("mail.smtp.auth", "true");               // 需要请求认证
        properties.put("mail.smtp.port", "587");                // 端口号
        //SSL 安全认证
//        properties.put("mail.smtp.socketFactory.class", "javax.net.ssl.SSLSocketFactory");
//        properties.put("mail.smtp.socketFactory.fallback", "false");
//        properties.put("mail.smtp.socketFactory.port", "465");
        //创建会话对象
        Session session = Session.getDefaultInstance(properties, new Authenticator() {
            @Override
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication("auoy23@qq.com", "jobwdewvtwxbbcii");
            }
        });
        session.setDebug(true);//可以查看详细的发送Log
        //创建一封邮件
        try {
            MimeMessage message = new MimeMessage(session);
            //From
            message.setFrom(new InternetAddress("auoy23@qq.com", "sqlexercise", "UTF-8"));
            //To
            message.setRecipient(MimeMessage.RecipientType.TO,
                    new InternetAddress(email, "sqlexercise 新用户", "UTF-8"));
            //主题
            message.setSubject("SQL Exercise 注册验证码", "UTF-8");
            //正文
            message.setContent("\n\n" +
                    "            <p>欢迎来到 SQL Exercise</p>\n" +
                    "            <p>您的网站账户注册验证码是：</p>\n" +
                    "        <p><span style=\"font-size: 24px; color: red\">" + code + "</span></p>" +
                    "            <p>验证码有效期为3分钟</p>", "text/html;charset=UTF-8");
            message.setSentDate(new Date());
            System.out.println(code);
            Transport transport = session.getTransport();
            transport.connect("799580275@qq.com", "jobwdewvtwxbbcii");
            transport.sendMessage(message, message.getAllRecipients());
            transport.close();

            // 在redis中缓存登录验证码，过期时间 3min
            String key = Constants.RedisKey.CODE_KEY_PREFIX + email;
            stringRedisTemplate.opsForValue().set(key, code, 3, TimeUnit.MINUTES);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return ResponseVO.success(Constants.Message.CODE_SEND_SUCCEED);
    }

    @Override
    public String generateMessageCode() {
        StringBuilder newCode = new StringBuilder();
        String all = "0123456789abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        for (int i = 0; i < 6; i++) {
            int num = (int) (Math.random() * 82);
            newCode.append(all.charAt(num));
        }
        return newCode.toString();
    }
}
