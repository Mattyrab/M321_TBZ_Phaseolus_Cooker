using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;

namespace CookerBeaners.Messaging;

public class EventPublisher : IDisposable
{
    private readonly IConnection _connection;
    private readonly IModel _channel;
    private readonly object _channelLock = new();

    public EventPublisher(IOptions<RabbitMqOptions> options)
    {
        var o = options.Value;
        var factory = new ConnectionFactory
        {
            HostName = o.Host,
            Port = o.Port,
            UserName = o.User,
            Password = o.Pass,
            AutomaticRecoveryEnabled = true,
        };

        _connection = factory.CreateConnection("CookerBeaners-publisher");
        _channel = _connection.CreateModel();
        _channel.ExchangeDeclare(RabbitMqOptions.ExchangeName, ExchangeType.Topic, durable: true);
    }

    public bool IsConnected => _connection.IsOpen && _channel.IsOpen;

    public void Publish(string routingKey, EventEnvelope envelope)
    {
        var body = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(envelope));

        // Cooking happens on a per-order delay task (see Program.cs), so unlike the
        // other services' publishers, this one can be called concurrently from
        // multiple threads -- RabbitMQ.Client's IModel isn't safe for that.
        lock (_channelLock)
        {
            var properties = _channel.CreateBasicProperties();
            properties.Persistent = true;
            properties.ContentType = "application/json";

            _channel.BasicPublish(
                exchange: RabbitMqOptions.ExchangeName,
                routingKey: routingKey,
                basicProperties: properties,
                body: body);
        }
    }

    public void Dispose()
    {
        _channel.Dispose();
        _connection.Dispose();
    }
}
