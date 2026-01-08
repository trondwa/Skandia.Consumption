using Azure.Storage.Queues;
using Skandia.Consumption.Shared.Models;
using System.Text;
using System.Text.Json;

namespace Skandia.Consumption.Ingest.Services;

public class AggregationQueuePublisher
{
    private readonly QueueClient _queueClient;

    public AggregationQueuePublisher(QueueClient queueClient)
    {
        _queueClient = queueClient;
    }

    public async Task EnqueueAsync(AggregationMessage message)
    {
        var json = JsonSerializer.Serialize(message);

        // Queue krever Base64
        var payload = Convert.ToBase64String(
            Encoding.UTF8.GetBytes(json));

        await _queueClient.SendMessageAsync(payload);
    }
}
