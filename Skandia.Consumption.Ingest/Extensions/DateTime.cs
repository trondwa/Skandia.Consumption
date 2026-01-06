namespace Skandia.Consumption.Ingest.Extensions;

public static class DateTimeExtensions
{
    public static DateTime TimeInOslo(this DateTime @this)
    {
        string id = "Central European Standard Time";
        TimeZoneInfo destinationTimeZone = TimeZoneInfo.FindSystemTimeZoneById(id);
        return TimeZoneInfo.ConvertTimeFromUtc(@this.ToUniversalTime(), destinationTimeZone);
    }

    public static long ToUnixTimestamp(this DateTime @this)
    {
        return (long)@this.Subtract(new DateTime(1970, 1, 1)).TotalSeconds;
    }
}
