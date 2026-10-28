using System.Text.Json;
using CookerBeaners.HealthChecks;
using CookerBeaners.Messaging;
using CookerBeaners.Models;
using Prometheus;

var builder = WebApplication.CreateBuilder(args);

builder.Services.Configure<RabbitMqOptions>(builder.Configuration.GetSection(RabbitMqOptions.SectionName));
builder.Services.AddSingleton<EventPublisher>();

var ordersCookingStarted = Metrics.CreateCounter("orders_cooking_started_total", "Total number of orders that started cooking.");
var ordersCooked = Metrics.CreateCounter("orders_cooked_total", "Total number of orders that finished cooking.");

builder.Services.AddSingleton<IReadOnlyList<ConsumerBinding>>(sp =>
{
    var publisher = sp.GetRequiredService<EventPublisher>();
    return new List<ConsumerBinding>
    {
        new("cooker.paid-orders", "payment.succeeded", envelope => HandlePaymentSucceededAsync(envelope, publisher, ordersCookingStarted, ordersCooked)),
    };
});
builder.Services.AddHostedService<EventConsumerHostedService>();

builder.Services.AddHealthChecks()
    .AddCheck("self", () => Microsoft.Extensions.Diagnostics.HealthChecks.HealthCheckResult.Healthy(), tags: ["live"])
    .AddCheck<RabbitMqHealthCheck>("rabbitmq", tags: ["ready"]);

var app = builder.Build();

app.UseHttpMetrics();
app.MapMetrics();

app.MapHealthChecks("/healthz/live", new() { Predicate = c => c.Tags.Contains("live") });
app.MapHealthChecks("/healthz/ready", new() { Predicate = c => c.Tags.Contains("ready") });

app.Run();

static Task HandlePaymentSucceededAsync(EventEnvelope envelope, EventPublisher publisher, Counter ordersCookingStarted, Counter ordersCooked)
{
    ordersCookingStarted.Inc();

    var cookedEnvelope = envelope with
    {
        EventId = Guid.NewGuid(),
        EventType = "order.cooked",
        OccurredAt = DateTimeOffset.UtcNow,
        Payload = JsonSerializer.SerializeToElement(new OrderCookedPayload()),
    };

    publisher.Publish("order.cooked", cookedEnvelope);
    ordersCooked.Inc();

    return Task.CompletedTask;
}
