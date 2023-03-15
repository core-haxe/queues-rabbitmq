package queues.rabbitmq;

import haxe.Serializer;
import haxe.Unserializer;
import promises.Promise;
import rabbitmq.ConnectionManager;
import rabbitmq.Message;
import rabbitmq.RabbitMQError;
import rabbitmq.RetryableQueue;

class RabbitMQQueue<T> implements IQueue<T> {
    private var _queue:RetryableQueue;

    private var _producerOnly:Bool = false;

    public function new() {
    }

    private var _onMessage:T->Promise<Bool>;
    public var onMessage(get, set):T->Promise<Bool>;
    private function get_onMessage():T->Promise<Bool> {
        return _onMessage;
    }
    private function set_onMessage(value:T->Promise<Bool>):T->Promise<Bool> {
        _onMessage = value;
        return value;
    }
    
    private var _config:Dynamic = null;
    public function config(config:Dynamic) {
        // TODO: validate or dont use Dynamic (somehow)
        _config = config;
        if (_config != null && _config.producerOnly != null) {
            _producerOnly = _config.producerOnly;
        }
    }

    private var _started:Bool = false;
    public function start():Promise<Bool> {
        return new Promise((resolve, reject) -> {
            if (_started) {
                resolve(true);
                return;
            }
            ConnectionManager.instance.getConnection(_config.brokerUrl).then(connection -> {
                _queue = new RetryableQueue({
                    connection: connection,
                    queueName: _config.queueName,
                    producerOnly: _producerOnly
                });
                return _queue.start();
            }).then(retryableQueue -> {
                if (!_producerOnly) {
                    retryableQueue.onMessage = onRabbitMQMessage;
                }
                _started = true;
                resolve(true);
            }, (error:RabbitMQError) -> {
                //connection.close();
                reject(error);
            });
    
        });
    }

    private function onRabbitMQMessage(message:Message) {
        var item:Dynamic = Unserializer.run(message.content.toString()); // TODO: probably want to make this pluggable
        _onMessage(item).then(success -> {
            if (success) {
                message.ack();
            } else{
                message.nack();
            }
        }, error -> {
            message.nack();
        });
    }

    public function enqueue(item:T) {
        var data = Serializer.run(item); // TODO: probably want to make this pluggable
        var message = new Message(data);
        _queue.publish(message);
    }

    public function requeue(item:T, delay:Null<Int> = null) {
        var data = Serializer.run(item); // TODO: probably want to make this pluggable
        var message = new Message(data);
        _queue.retry(message, delay);
    }

}