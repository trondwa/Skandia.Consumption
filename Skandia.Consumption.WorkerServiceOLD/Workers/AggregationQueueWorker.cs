using System.Text;
using System.Text.Json;
using Azure.Storage.Queues;
using Azure.Storage.Queues.Models;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Skandia.Consumption.Shared.Models;
using Skandia.Consumption.WorkerService.Services;

namespace Skandia.Consumption.WorkerService.Workers;

public sealed class AggregationQueueWorker : BackgroundService
{
    private readonly QueueClient _queueClient;
    private readonly ILogger<AggregationQueueWorker> _logger;
    private readonly IServiceProvider _services;

    public AggregationQueueWorker(
        QueueClient queueClient,
        ILogger<AggregationQueueWorker> logger,
        IServiceProvider services)
    {
        _queueClient = queueClient;
        _logger = logger;
        _services = services;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Aggregation queue worker started");

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var messages = await _queueClient.ReceiveMessagesAsync(
                    maxMessages: 1,
                    visibilityTimeout: TimeSpan.FromMinutes(5),
                    cancellationToken: stoppingToken);

                if (messages.Value.Length == 0)
                {
                    await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
                    continue;
                }

                var message = messages.Value[0];

                await ProcessMessageAsync(message, stoppingToken);

                await _queueClient.DeleteMessageAsync(
                    message.MessageId,
                    message.PopReceipt,
                    stoppingToken);
            }
            catch (OperationCanceledException)
            {
                // normal shutdown
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Unhandled error in queue worker");
                await Task.Delay(TimeSpan.FromSeconds(10), stoppingToken);
            }
        }
    }

    private async Task ProcessMessageAsync(
        QueueMessage queueMessage,
        CancellationToken ct)
    {
        var json = Encoding.UTF8.GetString(
            Convert.FromBase64String(queueMessage.MessageText));

        var message = JsonSerializer.Deserialize<AggregationMessage>(json);

        if (message is null)
        {
            _logger.LogWarning("Invalid aggregation message");
            return;
        }

        _logger.LogInformation(
            "Processing aggregation for mpid={Mpid}, {From} → {To}",
            message.Mpid,
            message.FromHour,
            message.ToHour);

        using var scope = _services.CreateScope();

        var processor = scope.ServiceProvider
            .GetRequiredService<AggregationProcessor>();

        await processor.ProcessAsync(message, ct);
    }
}