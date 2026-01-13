using Skandia.Consumption.Shared.Models;

namespace Skandia.Consumption.WorkerService.Models;

public sealed class InvalidAggregationMessage
{
    public AggregationMessage OriginalMessage { get; init; } = default!;
    public string Reason { get; init; } = default!;
    public string? Details { get; init; }
    public DateTime UtcTimestamp { get; init; } = DateTime.UtcNow;
    public string FunctionName { get; init; } = "ProcessAggregationMessage";
    public int? DeliveryId { get; init; }
}
