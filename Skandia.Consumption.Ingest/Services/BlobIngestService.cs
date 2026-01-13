
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Dapper;
using Npgsql;
using Skandia.Consumption.Ingest.Extensions;
using Skandia.Consumption.Ingest.Models;
using Skandia.Consumption.Shared.Helpers;
using Skandia.Consumption.Shared.Models;
using Skandia.DB;
using System.IO.Compression;
using System.Text.Json;

namespace Skandia.Consumption.Ingest.Services;


public sealed class BlobIngestService
{
    private readonly BlobServiceClient _blobServiceClient;
    private readonly AggregationQueuePublisher _aggregationQueue;
    private readonly IRepository<MeterValueData> _meterValueRepository;
    private readonly ILogger<BlobIngestService> _logger;

    public BlobIngestService(
                                BlobServiceClient blobServiceClient,
                                AggregationQueuePublisher aggregationQueue,
                                IRepository<MeterValueData> meterValueRepository,
                                ILogger<BlobIngestService> logger)
    {
        _blobServiceClient = blobServiceClient;
        _aggregationQueue = aggregationQueue;
        _meterValueRepository = meterValueRepository;
        _logger = logger;
    }

    public async Task ProcessAsync(Uri blobUri, CancellationToken ct = default)
    {
        var archivePrefix = "archive/";
        var archiveInPrefix = "archivein/";

        var path = blobUri.AbsolutePath.TrimStart('/');
        var parts = path.Split('/', 2);

        var containerName = parts[0];
        var blobName = parts[1];

        var blobClient = _blobServiceClient
            .GetBlobContainerClient(containerName)
            .GetBlobClient(blobName);

        if (blobName.Contains("_Out") &&
            !blobName.Contains(archivePrefix) &&
            !blobName.Contains(archiveInPrefix))
        {
            await ProcessOutFile(blobClient, archivePrefix, blobUri.ToString(), ct);
        }
        else if (blobName.Contains("_In") &&
                 !blobName.Contains(archivePrefix) &&
                 !blobName.Contains(archiveInPrefix))
        {
            await Archive(blobClient, archiveInPrefix, ct);
        }
    }

    private async Task Archive(BlobClient blobClient, string archiveInPrefix, CancellationToken ct)
    {
        var containerName = "metervalues";
        var containerClient = _blobServiceClient.GetBlobContainerClient(containerName);
        var destinationBlobClient = containerClient.GetBlobClient(archiveInPrefix + blobClient.Name);
        // Copy the blob to the new destination
        await destinationBlobClient.StartCopyFromUriAsync(blobClient.Uri);

        // Wait for the copy operation to complete
        var copyStatus = await WaitForCopyCompletionAsync(destinationBlobClient);

        if (copyStatus == CopyStatus.Success)
        {
            // If copy is successful, delete the original blob
            await blobClient.DeleteIfExistsAsync();
        }
        else
        {
            // Handle the case where copy operation failed
            // Logg error
        }
    }

    private async Task ProcessOutFile(BlobClient blobClient, string archivePrefix, string blobUrl, CancellationToken ct)
    {
        var doArchive = true;

        await using var stream = await blobClient.OpenReadAsync(cancellationToken: ct);
        await using var gzipStream = new GZipStream(stream, CompressionMode.Decompress);
        using var reader = new StreamReader(gzipStream);

        var content = await reader.ReadToEndAsync(ct);

        var MeterValueInfo =
            JsonSerializer.Deserialize<MeterValueInfo>(content);

        if (MeterValueInfo == null)
            return;

        var source = Guid.NewGuid().ToString();

        var newReadings = CreateMeterValuesDataObject(MeterValueInfo, source);
        var oldReadings = await GetMeterValuesByMpid(MeterValueInfo.MeteringPointId, newReadings.Min(r => r.Hour), newReadings.Max(r => r.Hour));

        newReadings = newReadings
            .Where(r => !oldReadings.Any(r2 =>
                r2.hour == r.Hour && r2.value == r.Value))
            .ToList();

        if (newReadings.Any())
        {
            BulkInsertBinaryImporter(newReadings);

            try
            {
                await _aggregationQueue.EnqueueAsync(
                new AggregationMessage
                {
                    Mpid = newReadings.First().Mpid,
                    FromHour = newReadings.Min(r => r.Hour),
                    ToHour = newReadings.Max(r => r.Hour),
                    Source = source
                });

            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to enqueue aggregation message");
                // ingest must proceed even if enqueue fails
            }
        }

        if (doArchive)
            await Archive(blobClient, archivePrefix, ct);


    }


    private static async Task<CopyStatus> WaitForCopyCompletionAsync(BlobBaseClient blobClient)
    {
        do
        {
            var properties = await blobClient.GetPropertiesAsync();
            if (properties.Value.CopyStatus == CopyStatus.Success)
            {
                return CopyStatus.Success;
            }
            else if (properties.Value.CopyStatus == CopyStatus.Failed || properties.Value.CopyStatus == CopyStatus.Aborted)
            {
                return CopyStatus.Failed;
            }

            await Task.Delay(1000); // Wait for a moment before checking again
        } while (true);
    }

    private static List<MeterValueData> CreateMeterValuesDataObject(MeterValueInfo MeterValueInfo, string source)
    {
        var MeterValuesList = new List<MeterValueData>();

        foreach (var MeterValue in MeterValueInfo.MeterReadings)
        {
            MeterValuesList.Add(
            new MeterValueData
            {
                Created = DateTime.UtcNow,
                Value = (decimal)MeterValue.Value,
                Source = source,
                Hour = MeterValue.Period.Start.TimeInOslo(),
                Direction = MeterValue.Direction == 0 ? "In" : "Out",
                Mpid = MeterValueInfo.MeteringPointId,
                Quality = GetEnumString<MeterValueQuality>(MeterValue.Quality),
            });
        }

        return MeterValuesList;
    }

    private static string GetEnumString<T>(int value) where T : Enum
    {
        if (Enum.IsDefined(typeof(T), value))
        {
            return Enum.GetName(typeof(T), value);
        }
        else
        {
            // Handle the case where the integer value doesn't match any enum value
            return "Unknown";
        }
    }

    private async Task<List<MeterValueItem>> GetMeterValuesByMpid(string mpid, DateTime fromHour, DateTime toHour)
    {
        var conn = _meterValueRepository.UnitOfWork.GetConnection();
        var sql = @$"select hour, value from consumption.raw_data where mpid = @mpid and hour >= @fromHour and hour <= @toHour";
        var result = await conn.QueryAsync<MeterValueItem>(sql, new { mpid, fromHour, toHour });

        return result.ToList();
    }


    private void BulkInsertBinaryImporter(List<MeterValueData> meterValues)
    {
        var conn = (NpgsqlConnection)_meterValueRepository.UnitOfWork.GetConnection();

        using (var writer = conn.BeginBinaryImport(Storage.GetBulkString(typeof(MeterValueData))))
        {
            foreach (var dataRow in meterValues)
            {
                writer.StartRow();
                writer.Write(dataRow.Created);
                writer.Write(dataRow.Source);
                writer.Write(dataRow.Mpid);
                writer.Write(dataRow.Direction);
                writer.Write(dataRow.Hour);
                writer.Write(dataRow.Value);
                writer.Write(dataRow.Quality);
            }
            writer.Complete();
        }
    }

}

