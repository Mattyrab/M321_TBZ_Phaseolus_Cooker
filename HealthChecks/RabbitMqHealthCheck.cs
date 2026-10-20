using Microsoft.Extensions.Diagnostics.HealthChecks;
using CookerBeaners.Messaging;

namespace CookerBeaners.HealthChecks;

public class RabbitMqHealthCheck(EventPublisher publisher) : IHealthCheck
{
    public Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        var result = publisher.IsConnected
            ? HealthCheckResult.Healthy("RabbitMQ connection open")
            : HealthCheckResult.Unhealthy("RabbitMQ connection closed");

        return Task.FromResult(result);
    }
}
