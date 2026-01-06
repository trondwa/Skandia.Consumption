namespace Skandia.Consumption.Ingest.Models;

public enum MeterValueQuality
{
    NoDelivery = -1,
    Missing = 0,
    Incomplete = 1,
    Rejected = 2,
    Unknown = 3,
    Temporary = 4,
    Calculated = 5,
    Estimated = 6,
    Metered = 7
}

