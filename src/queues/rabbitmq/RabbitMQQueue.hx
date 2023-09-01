package queues.rabbitmq;

import promises.Promise;
import rabbitmq.ConnectionManager;
import rabbitmq.Message;
import rabbitmq.RabbitMQError;
import rabbitmq.RetryableQueue;
import serializers.ISerializable;

class RabbitMQQueue<T> implements IQueue<T> {
    private var _queue:RetryableQueue;

    private var _producerOnly:Bool = false;

    public function new() {
    }

    private var _name:String = null;
    public var name(get, set):String;
    private function get_name() {
        return _name;
    }
    private function set_name(value:String):String {
        _name = value;
        return value;
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

    public function stop():Promise<Bool> {
        return new Promise((resolve, reject) -> {
            //_queue.onMessage = null;
            _queue.close().then(success -> {
                resolve(success);
            });
        });
    }

    private function onRabbitMQMessage(message:Message) {
        var item:Dynamic = null;
        if (message.headers != null && message.headers.get("serializer") != null) {
            var serializerClass = message.headers.get("serializer");
            item = Type.createInstance(Type.resolveClass(serializerClass), []);
            if ((item is ISerializable)) {
                cast(item, ISerializable).unserialize(message.content.toString());
            }
        } else {
            item = haxe.Unserializer.run(message.content.toString()); // TODO: probably want to make this pluggable
        }
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
        var headers:Map<String, Any> = [];
        var data:String = null;
        if ((item is ISerializable)) {
            headers.set("serializer", Type.getClassName(Type.getClass(item)));
            data = cast(item, ISerializable).serialize();
        } else{
            data = haxe.Serializer.run(item);
        }
        var message = new Message(data, headers);
        _queue.publish(message);
    }

    public function requeue(item:T, delay:Null<Int> = null) {
        var data:String = null;
        if ((item is ISerializable)) {
            data = cast(item, ISerializable).serialize();
        } else{
            data = haxe.Serializer.run(item);
        }
        var message = new Message(data);
        _queue.retry(message, delay);
    }

}