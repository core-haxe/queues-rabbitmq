package queues.rabbitmq;

import rabbitmq.Connection;
import rabbitmq.MessageOptions;
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
    
    private var _onMessageWithProperties:T->Map<String, Any>->Promise<Bool>;
    public var onMessageWithProperties(get, set):T->Map<String, Any>->Promise<Bool>;
    private function get_onMessageWithProperties():T->Map<String, Any>->Promise<Bool> {
        return _onMessageWithProperties;
    }
    private function set_onMessageWithProperties(value:T->Map<String, Any>->Promise<Bool>):T->Map<String, Any>->Promise<Bool> {
        _onMessageWithProperties = value;
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
                connection.listenFor("error", onConnectionError);
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
                if (ConnectionManager.autoReconnect && !_producerOnly && error.message.indexOf("connect ECONNREFUSED") != -1) {  // TODO: flakey error recognition
                    attemptReconnect().then(reconnected -> {
                        resolve(reconnected);
                    }, error -> {
                        reject(error);
                    });
                } else {
                    reject(error);
                }
            });
        });
    }

    private function onConnectionError(error:Any, connection:Connection) {
        var errorString = Std.string(error);
        if (errorString == "Error: read ECONNRESET") { // TODO: flakey error recognition
            connection.unlistenFor("error", onConnectionError);
            // note that we will only try to reconnect in this queue object when its not a producer only
            // the reasoning is that "publish" code has its only retry mechanism (similar to this one)
            // but it happens at the point of failure (ie, publish), this means we dont have to make any
            // checks in the enqueue in this class and can let the producer handle its own reconnection
            // internally and re-publish its messages (rather than having to cache them here or lose them)
            if (ConnectionManager.autoReconnect && !_producerOnly) {
                attemptReconnect();
            }
        }
    }

    private function attemptReconnect():Promise<Bool> {
        return new Promise((resolve, reject) -> {
            haxe.Timer.delay(() -> {
                _attemptReconnect(resolve, reject);
            }, ConnectionManager.autoReconnectIntervalMS);
        });
    }

    private function _attemptReconnect(resolve:Bool->Void, reject:Any->Void) {
        trace("connection dropped, attempting to reconnect");
        // we'll force a new connection here, not technically need since the connection manager
        // will automatically clean itself up, but this is an added level of being sure
        ConnectionManager.instance.getConnection(_config.brokerUrl, true).then(connection -> {
            trace("reconnected successfully after dropped connection");
            _started = false;
            return start();
        }).then(result -> {
            resolve(true);
        }, error -> {
            haxe.Timer.delay(() -> {
                _attemptReconnect(resolve, reject);
            }, ConnectionManager.autoReconnectIntervalMS);
        });
    }

    public function stop():Promise<Bool> {
        return new Promise((resolve, reject) -> {
            _queue.onMessage = null;
            _queue.close().then(success -> {
                resolve(success);
            }, error -> {
                reject(error);
            });
        });
    }

    private function onRabbitMQMessage(message:Message) {
        var item:Dynamic = null;
        if (message.headers != null && message.headers.get("serializer") != null) {
            var serializerClass:String = message.headers.get("serializer");

            #if nodejs
            try {
                item = Type.createInstance(Type.resolveClass(serializerClass), []);
            } catch (e:Dynamic) { }

            if (item == null) {
                var ref = js.Syntax.code("global");
                var parts = serializerClass.split(".");
                for (part in parts) {
                    ref = js.Syntax.code("{0}[{1}]", ref, part);
                    if (ref == null) {
                        break;
                    }
                }
                if (ref != null) {
                    item = js.Syntax.code("new {0}", ref);
                }
            }
            #else
            item = Type.createInstance(Type.resolveClass(serializerClass), []);
            #end

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
        } else {
            message.nack();
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
        var options:MessageOptions = { }
        if (properties != null) {
            options.replyTo = properties.get("replyTo");
        }
        var message = new Message(data, headers, options);
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