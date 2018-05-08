<?php
/**
 * rabbitMQ 消息队列
 * (只负责入列)
 */

use Think\Log;
use Think\Exception;

class RabbitMQ
{
    //交换机和队列的关联字
    private $queue_name;

    //交换机对象
    private $exchange;

    //连接对象
    private $conn;

    private $queue;
    private $envelope;

    public function __construct(array $config=array())
    {
        if(is_array($config) and !empty($config)){
            $conn_conf=array(
                'host' => $config['HOST'],
                'port' => $config['PORT'],
                'login' => $config['USERNAME'],
                'password' => $config['PASSWORD'],
                'vhost'=>$config['VHOST'],'/',
            );

            $this->queue_name=$config['QUEUE_NAME'];
            $exchange_name=$config['EXCHANGE_NAME'];
        }else{
            $conn_conf=array(
                'host' => C('MQ_HOST'),
                'port' => C('MQ_PORT'),
                'login' => C('MQ_USERNAME'),
                'password' => C('MQ_PASSWORD'),
                'vhost'=>C('MQ_VHOST'),
            );

            $this->queue_name=C('MQ_QUEUE_NAME');
            $exchange_name=C('MQ_EXCHANGE_NAME');
        }

        try{
            //创建连接和channel
            $this->conn = new AMQPConnection($conn_conf);
            if (!$this->conn->connect()) {
                //记录日志
                $msg="RabbitMQ|服务器IP[{$conn_conf['host']}]|端口[{$conn_conf['port']}]|用户名[{$conn_conf['login']}]|服务连接失败!\r\n";
                Log::write($msg,Log::ERR,'File',C('LOG_PATH').date('y-m-d').'.log');

                throw_exception($msg);
            }

            $channel = new AMQPChannel($this->conn);

            //创建交换机
            $this->exchange = new AMQPExchange($channel);

            $this->exchange->setName($exchange_name);
            $this->exchange->setType(AMQP_EX_TYPE_DIRECT); //direct类型
            $this->exchange->setFlags(AMQP_DURABLE); //持久化

            //声明
            $this->exchange->declareExchange();

//            echo "Exchange Status:".$ex->declareExchange()."\n";

            //创建队列
            $this->queue = new AMQPQueue($channel);
            $this->queue->setName($this->queue_name);
            $this->queue->bind($exchange_name, $this->queue_name);
            $this->queue->setFlags(AMQP_DURABLE);

            //声明
            // $this->queue->declareQueue();

//            echo "Message Total:".$q->declareQueue()."\n";
        }catch (Exception $e){
            $msg="RabbitMQ|服务器IP[{$conn_conf['host']}]|端口[{$conn_conf['port']}]|服务连接异常[{$e->getMessage()}]!\r\n";
            Log::write($msg,Log::ERR,'File',C('LOG_PATH').date('y-m-d').'.log');

            throw_exception($e->getMessage());
        }
    }

    public function publish($message=''){
        $re=$this->exchange->publish($message,$this->queue_name);
        $this->conn->disconnect();
        return $re;
    }

    public function get()
    {
        if ($arr = $this->queue->get()) {
            $this->queue->ack($arr->getDeliveryTag());
            return $arr->getBody();
        }

        return false;
    }
}
