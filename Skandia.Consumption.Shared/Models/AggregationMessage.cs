
namespace Skandia.Consumption.Shared.Models
{
    public sealed class AggregationMessage
    {
        public string Mpid { get; init; } = default!;

        /// <summary>
        /// First affected hour (inclusive)
        /// </summary>
        public DateTime FromHour { get; init; }

        /// <summary>
        /// Last affected hour (inclusive)
        /// </summary>
        public DateTime ToHour { get; init; }

        /// <summary>
        /// Source of the message (e.g. "ingest")
        /// </summary>
        public string Source { get; init; } = default!;
    }

}
