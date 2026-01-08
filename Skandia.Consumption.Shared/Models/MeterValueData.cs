using Dapper;

namespace Skandia.Consumption.Shared.Models;

[Table("raw_data", Schema = "consumption")]
public class MeterValueData
{
    [Key]
    [Column("id")]
    public int Id { get; set; }
    [Column("created")]
    public DateTime Created { get; set; } = DateTime.UtcNow;
    [Column("sourcebloburl")]
    public string SourceBlobUrl { get; set; }
    [Column("mpid")]
    public string Mpid { get; set; }
    [Column("direction")]
    public string Direction { get; set; }
    [Column("hour")]
    public DateTime Hour { get; set; }
    [Column("value")]
    public decimal Value { get; set; }
    [Column("quality")]
    public string Quality { get; set; }
}

