
namespace Skandia.Consumption.WorkerService.Models
{
    public sealed class PriceData
    {
        public int DeliveryId { get; init; }
        public DateTime Hour { get; init; }
        public decimal Price { get; init; }
        public decimal? Margin { get; init; }
        public decimal Tax { get; init; }

    }
}
