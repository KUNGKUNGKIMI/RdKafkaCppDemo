#include <iostream>
#include <cstdlib>
#include <string>
#include <vector>
#include <csignal>
#include <librdkafka/rdkafkacpp.h>


static bool run = true;

static void sigterm(int sig) {
    run = false;
}

//消息处理函数
void msgConsume(RdKafka::Message * message, void * opaque) {
    switch (message->err())
    {
    case RdKafka::ERR__TIMED_OUT:
        
        break;
    case RdKafka::ERR_NO_ERROR:
        //成功读取到消息
        std::cout << "[+]收到消息! "
            << " |key: " << (message->key() ? *message->key(): "None")
            << " |Payload: " << static_cast<const char*>(message->payload())
            << " |Partition: " << message->partition()
            << " |Offset: " << message->offset() << std::endl;
        break;
    case RdKafka::ERR__PARTITION_EOF:
        std::cout << "[*]触及分区末尾" << std::endl;
        break;
    default:
        //其他错误
        std::cerr << "[-]消费错误: " << message->errstr() << std::endl;
        run = false;
        break;
    }
}


int main(){
    using namespace std;
    string brokers = "localhost:9092";
    string groupId = "test_cpp_group";
    vector<string> topics = {"test_topic"};

    string errStr;
    signal(SIGINT, sigterm);
    signal(SIGTERM, sigterm);

    //创建全局配置
    RdKafka::Conf* conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    if (conf->set("bootstrap.servers", brokers, errStr) != RdKafka::Conf::CONF_OK)
    {
        std::cerr << errStr << std::endl;
        exit(1);
    }

    if (conf->set("group.id", groupId, errStr) != RdKafka::Conf::CONF_OK)
    {
        std::cerr << errStr << std::endl;
        exit(1);
    }

    if (conf->set("auto.offset.reset", "earliest", errStr) != RdKafka::Conf::CONF_OK)
    {
        std::cerr << errStr << std::endl;
        exit(1);
    }

    //创建consumer实例
    auto * consumer = RdKafka::KafkaConsumer::create(conf, errStr);
    if (!consumer)
    {
        std::cerr << "[-]创建consumer失败: " << errStr << std::endl;
        exit(1);
    }
    delete conf;

    std::cout << "[+]消费者初始化成功..." << std::endl;
    
    //订阅topics
    RdKafka::ErrorCode err = consumer->subscribe(topics);
    if (err != RdKafka::ERR_NO_ERROR)
    {
        std::cerr << "[-]订阅失败: " << RdKafka::err2str(err) << std::endl;
        delete consumer;
        exit(1);
    }

    //循环拉取消息
    while (run)
    {
        //阻塞等待1s
        RdKafka::Message* message = consumer->consume(1000);
        msgConsume(message, nullptr);
        delete message;
    }
    
    //关闭消费者并释放资源
    std::cout << "\n[+]正在关闭消费者并提交位移..." << std::endl;
    consumer->close();
    delete consumer;

    //销毁kafka全局释放的资源
    RdKafka::wait_destroyed(5000);
    return EXIT_SUCCESS;
}



