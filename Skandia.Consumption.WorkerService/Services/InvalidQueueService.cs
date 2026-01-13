using System.Text;
using System.Text.Json;
using Azure.Storage.Queues;
using Skandia.Consumption.WorkerService.Models;

namespace Skandia.Consumption.WorkerService.Services;

public sealed class InvalidQueueService
{
    private readonly QueueClient _queueClient;

    public InvalidQueueService(QueueClient queueClient)
    {
        _queueClient = queueClient;
    }

    public async Task SendAsync(
        InvalidAggregationMessage invalid,
        CancellationToken ct = default)
    {
        // create if missing (safe)
        await _queueClient.CreateIfNotExistsAsync(cancellationToken: ct);

        var json = JsonSerializer.Serialize(invalid, new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = false
        });

        // Queue messages must be Base64 unless configured otherwise
        var base64 = Convert.ToBase64String(Encoding.UTF8.GetBytes(json));

        await _queueClient.SendMessageAsync(
            base64,
            cancellationToken: ct);
    }
}
