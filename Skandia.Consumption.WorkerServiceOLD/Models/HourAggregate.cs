using Dapper;

namespace Skandia.Consumption.WorkerService.Models;

[Table("hour_aggregates", Schema = "consumption")]
public class HourAggregateData
{
    [Column("created")]
    public DateTime Created { get; init; }
    [Column("deliveryid")]
    public int DeliveryId { get; init; }
    [Column("mpid")]
    public string Mpid { get; init; } = default!;
    [Column("date")]
    public DateTime Date { get; init; }
    [Column("price")]
    public decimal Price { get; init; }
    [Column("consumption")]
    public decimal Consumption { get; init; }
    [Column("cost")]
    public decimal Cost { get; init; }
    [Column("actual")]
    public bool Actual { get; init; }
}

