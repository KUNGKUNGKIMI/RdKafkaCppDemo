#include "service.h"
#include <iostream>
#include <string>
#include <cstdio>
#include <csignal>
#include <cstdlib>

void ExampleDeliveryReportCb::dr_cb(RdKafka::Message &message)
{
    if (message.err())
    {
        std::cerr << "[-]消条投递失败: " << message.errstr() << std::endl;        
    }
    else {
        std::cout << "[+]消息投递成功! TPOIC: " << message.topic_name() <<
        " | Partition: " << message.partition() << " | Offset: " << message.offset() << std::endl;
    }
    
}

int main() {
    using namespace std;

    string brokers = "localhost:9092";
    string topicName = "test_topic";
    string errStr;

    //创建配置对象
    auto* conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);

    //设置broker地址
    if (conf->set("bootstrap.servers", brokers, errStr) != RdKafka::Conf::CONF_OK) {
        std::cerr << "[-]设置bootstrap.servers失败: " << errStr << std::endl;
        exit(1);
    }

    //注册投递回调
    ExampleDeliveryReportCb ex_dr_cb;
    if (conf->set("dr_cb", &ex_dr_cb, errStr) != RdKafka::Conf::CONF_OK)
    {
        std::cerr << "[-]设置投递失败: " << errStr << std::endl;
        exit(1);
    }

    //创建Producer实例
    auto * producer = RdKafka::Producer::create(conf, errStr);
    if(!producer) {
        std::cerr << "[-]kafka生产者创始化失败: " << errStr << std::endl;
    }
    
    delete conf;
    std::cout << "[+]kafka生产者创始化成功,输入消息按回车发送(输入'q'退出)" << std::endl;

    //循环读取输入并发送
    for (string line; std::getline(std::cin, line);)
    {
        if (line == "q")
        {
            break;   
        }
        if (line.empty())
        {
            continue;
        }
        
        //异步发送消息
        RdKafka::ErrorCode err = producer->produce(topicName,
            RdKafka::Topic::PARTITION_UA,
            RdKafka::Producer::RK_MSG_COPY,
            const_cast<char*>(line.c_str()), line.size(),
            nullptr, 0,
            0,
            nullptr);
       
        if (err != RdKafka::ERR_NO_ERROR)
        {
            std::cerr << "[-]发送排队失败: " << RdKafka::err2str(err) << std::endl;
        }
        else {
            std::cout << "[+]消息发送成功!..." << std::endl; 
        }

        //轮询触发dr_cb回调
        producer->poll(0);
        
    }

    //退出前阻塞等待所有排队消息发送完成
    std::cout << "[*]正在冲刷剩余队列..." << std::endl;
    producer->flush(5000);

    if (producer->outq_len() > 0)
    {
        std::cerr << "[-]警告:还有 " << producer->outq_len() << " 条消息为完成发送" << std::endl;
    }
    
    delete producer;

    return EXIT_SUCCESS;
}