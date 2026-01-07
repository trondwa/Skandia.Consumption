namespace Skandia.Consumption.Ingest.Models
{
    public class MeterValue
    {
        public string Id { get; set; }
        public Period Period { get; set; }
        public double Value { get; set; }
        public int Quality { get; set; }
        public int Direction { get; set; }
        public int Unit { get; set; }
        public int Resolution { get; set; }
        public string MeteringPointId { get; set; }
        public DateTime Updated { get; set; }
    }

    public class Period
    {
        public DateTime Start { get; set; }
        public DateTime End { get; set; }
    }

    public class MeterValueInfo
    {
        public string Id { get; set; }
        public string MeteringPointId { get; set; }
        public List<MeterValue> MeterValues { get; set; }
        public string Period { get; set; }
        public int Direction { get; set; }
        public DateTime LastUpdated { get; set; }
        public DateTime PeriodStart { get; set; }
        public DateTime PeriodEnd { get; set; }
        public int Count { get; set; }
        public int Expected { get; set; }
    }

    public class MeterValueItem
    {
        public DateTime hour { get; set; }
        public decimal value { get; set; }
        public string sourcebloburl { get; set; }
    }
}
