# queues-rabbitmq
rabbitmq queue plugin for [__queues-core__](https://github.com/core-haxe/queues-core)

# basic usage

```haxe
var queue:IQueue<Int> = QueueFactory.createQueue(QueueFactory.RABBITMQ_QUEUE, {
    brokerUrl: "amqp://localhost",
    queueName: "my-http-request-queue"
});
...
```

See [__queues-core__](https://github.com/core-haxe/queues-core) for further information on how to use `IQueue<T>`

_Note: the act of including this haxelib in your project automatically registers its type with `QueueFactory`_
