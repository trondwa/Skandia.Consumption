
namespace Skandia.Consumption.WorkerService.Models
{
    public class ConsumptionItem
    {
        public int Period { get; set; }
        public decimal Energy { get; set; }
        public decimal Cost { get; set; }
        public DateTime? Date { get; set; }
    }

    public enum ResolutionType { Hour, Day, Month }
}
