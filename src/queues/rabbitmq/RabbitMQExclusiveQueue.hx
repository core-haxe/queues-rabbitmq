package queues.rabbitmq;

import haxe.io.Bytes;
import promises.Promise;
import rabbitmq.ConnectionManager;
import rabbitmq.RabbitMQError;
import rabbitmq.Queue;
import rabbitmq.Message;
import serializers.ISerializable;
import rabbitmq.Channel;

class RabbitMQExclusiveQueue<T> implements IQueue<T> {
    private var _queue:Queue;
    private var _channel:Channel;

    public function new() {
    }

    public var name(get, set):String;
    private function get_name() {
        if (_queue != null) {
            return _queue.name;
        }
        return _config.queueName;
    }
    private function set_name(value:String):String {
        if (_queue != null) {
            return value;
        }
        if (_config == null) {
            _config = {};
        }
        _config.queueName = value;
        return value;
    }

    private var _onMessage:T->Promise<Bool>;
    public var onMessage(get, set):T->Promise<Bool>;
    private function get_onMessage():T->Promise<Bool> {
        return _onMessage;
    }
    private function set_onMessage(value:T->Promise<Bool>):T->Promise<Bool> {
        _onMessage = value;
        _queue.onMessage = onRabbitMQMessage;
        _queue.startConsuming();
        return value;
    }
    
    private var _onMessageWithProperties:T->Map<String, Any>->Promise<Bool>;
    public var onMessageWithProperties(get, set):T->Map<String, Any>->Promise<Bool>;
    private function get_onMessageWithProperties():T->Map<String, Any>->Promise<Bool> {
        return _onMessageWithProperties;
    }
    private function set_onMessageWithProperties(value:T->Map<String, Any>->Promise<Bool>):T->Map<String, Any>->Promise<Bool> {
        _onMessageWithProperties = value;
        _queue.startConsuming();
        return value;
    }

    private var _config:Dynamic = null;
    public function config(config:Dynamic) {
        // TODO: validate or dont use Dynamic (somehow)
        _config = config;
    }
 
    private var _started:Bool = false;
    public function start():Promise<Bool> {
        return new Promise((resolve, reject) -> {
            if (_started) {
                resolve(true);
                return;
            }

            if (_config.queueName == null || _config.queueName == "") {
                ConnectionManager.instance.getConnection(_config.brokerUrl).then(connection -> {
                    connection.createChannel(false);
                }).then(result -> {
                    return result.channel.createQueue(_config.queueName, {
                        exclusive: true
                    });
                }).then(result -> {
                    _queue = result.queue;
                    resolve(true);
                }, (error:RabbitMQError) -> {
                    //connection.close();
                    reject(error);
                });
            } else {
                ConnectionManager.instance.getConnection(_config.brokerUrl).then(connection -> {
                    connection.createChannel(false);
                }).then(result -> {
                    _channel = result.channel;
                    resolve(true);
                }, (error:RabbitMQError) -> {
                    //connection.close();
                    reject(error);
                });
            }
        });
    }
    public function stop():Promise<Bool> {
        return new Promise((resolve, reject) -> {
            resolve(true);
            /*
            _queue.close().then(success -> {
                resolve(success);
            });
            */
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

        if (_onMessage != null) {
            _onMessage(item).then(success -> {
                if (success) {
                    message.ack();
                } else{
                    message.nack();
                }
            }, error -> {
                message.nack();
            });
        } else if (_onMessageWithProperties != null) {
            _onMessageWithProperties(item, message.properties).then(success -> {
                if (success) {
                    message.ack();
                } else{
                    message.nack();
                }
            }, error -> {
                message.nack();
            });
        }
    }

    public function enqueue(item:T, properties:Map<String, Any> = null) {
        var headers:Map<String, Any> = [];
        var data:String = null;
        if ((item is ISerializable)) {
            headers.set("serializer", Type.getClassName(Type.getClass(item)));
            data = cast(item, ISerializable).serialize();
        } else{
            data = haxe.Serializer.run(item);
        }

        _channel.sendToQueue(this.name, Bytes.ofString(data));
    }

    public function requeue(item:T, delay:Null<Int> = null) {
        throw "not implemented";
    }
}