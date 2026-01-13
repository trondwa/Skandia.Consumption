
namespace Skandia.Consumption.WorkerService.Models;

public sealed class HourAggregate
{
    public DateTime Created { get; init; }
    public int DeliveryId { get; init; }
    public string Mpid { get; init; } = default!;
    public DateTime Date { get; init; }
    public decimal Price { get; init; }
    public decimal Consumption { get; init; }
    public decimal Cost { get; init; }
    public bool Actual { get; init; }
}


