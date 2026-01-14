using Azure;
using Azure.Identity;
using Azure.Security.KeyVault.Secrets;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Queues;
using Skandia.Consumption.Shared.Models;
using System.Net;
using System.Text;
using System.Text.Json;

static string? GetEnv(string name)
{
    var v = Environment.GetEnvironmentVariable(name);
    return string.IsNullOrWhiteSpace(v) ? null : v;
}

static async Task<string> GetSecretAsync(string vaultUri, string secretName)
{
    var client = new SecretClient(new Uri(vaultUri), new DefaultAzureCredential());
    var secret = await client.GetSecretAsync(secretName);
    return secret.Value.Value;
}


// ------------------------------------------------------------
// CONFIG
// ------------------------------------------------------------
//
// Required:
//   BLOB_CONNECTIONSTRING
//   BLOB_CONTAINER_NAME
//   QUEUE_CONNECTIONSTRING
//
// Optional:
//   QUEUE_NAME (default aggregation-queue)
//   PREFIX (default 2026/)
//   SEND_DELAY_MS (default 5)
//   MAX_MESSAGES (default 0 = no limit)
//

var vaultUri = "https://key-app-skandia-prod.vault.azure.net/";

// Secret names (defaults)
var kvBlobConnSecret = "AzureWebJobsBlobStorageUC";
var kvQueueConnSecret = "AzureWebJobsBlobStorageUC";

// If Key Vault is configured, use it
string blobConnStr;
string queueConnStr;
string containerName;

var ingestUrl = GetEnv("INGEST_URL") ?? "https://app-consumption-skandia-prod.azurewebsites.net/api/eventgrid/ingest"; // "https://localhost:7228/api/eventgrid/ingest";
var maxBlobs = int.TryParse(GetEnv("MAX_BLOBS"), out var m) ? m : 0;
var maxRetries = int.TryParse(GetEnv("MAX_RETRIES"), out var r) ? r : 5;

if (!string.IsNullOrWhiteSpace(vaultUri))
{
    Console.WriteLine("Using Key Vault for configuration...");
    blobConnStr = await GetSecretAsync(vaultUri!, kvBlobConnSecret);
    queueConnStr = await GetSecretAsync(vaultUri!, kvQueueConnSecret);
    containerName = "metervalues";
}
else
{
    Console.WriteLine("Using ENV variables for configuration...");
    blobConnStr = GetEnv("AzureWebJobsBlobStorageUC")
                  ?? throw new Exception("Missing env var: AzureWebJobsBlobStorageUC");

    queueConnStr = GetEnv("AzureWebJobsBlobStorageUC")
                   ?? throw new Exception("Missing env var: AzureWebJobsBlobStorageUC");

    containerName = "aggregation-queue";
}

var queueName = GetEnv("QUEUE_NAME") ?? "aggregation-queue";
var prefix = GetEnv("PREFIX") ?? "2026/";
var sendDelayMs = 5;
var maxMessages = 0;

Console.WriteLine("=== Backfill Publisher ===");
Console.WriteLine($"Container: {containerName}");
Console.WriteLine($"Prefix: {prefix}");
Console.WriteLine($"Queue: {queueName}");
Console.WriteLine($"Send delay: {sendDelayMs}ms");
Console.WriteLine($"Max messages: {(maxMessages == 0 ? "no limit" : maxMessages)}");
Console.WriteLine();

// ------------------------------------------------------------
// CLIENTS
// ------------------------------------------------------------
var blobService = new BlobServiceClient(blobConnStr);
var container = blobService.GetBlobContainerClient(containerName);

using var http = new HttpClient
{
    Timeout = TimeSpan.FromMinutes(5)
};


// ------------------------------------------------------------
// HELPERS
// ------------------------------------------------------------


static bool ShouldSkip(BlobItem blob)
{
    var name = blob.Name;

    if (name.EndsWith("/")) return true;

    if (name.Contains("/archive/", StringComparison.OrdinalIgnoreCase)) return true;
    if (name.Contains("/archivein/", StringComparison.OrdinalIgnoreCase)) return true;

    if (name.StartsWith("archive/", StringComparison.OrdinalIgnoreCase)) return true;
    if (name.StartsWith("archivein/", StringComparison.OrdinalIgnoreCase)) return true;

    return false;
}

static string MakeEventGridBlobCreatedPayload(
    string blobUrl,
    string containerName,
    string blobName)
{
    // subject skal ligne på Event Grid sin subject
    // typisk: /blobServices/default/containers/<container>/blobs/<blobpath>
    var subject = $"/blobServices/default/containers/{containerName}/blobs/{blobName}";

    var evt = new[]
    {
        new
        {
            id = Guid.NewGuid().ToString(),
            eventType = "Microsoft.Storage.BlobCreated",
            subject = subject,
            eventTime = DateTime.UtcNow.ToString("O"),
            dataVersion = "1.0",
            metadataVersion = "1",
            data = new
            {
                api = "PutBlob",
                clientRequestId = Guid.NewGuid().ToString(),
                requestId = Guid.NewGuid().ToString(),
                eTag = "0x0",
                contentType = "application/octet-stream",
                contentLength = 0,
                blobType = "BlockBlob",
                url = blobUrl,
                sequencer = "00000000000000000000000000000000000000000000000000",
                storageDiagnostics = new
                {
                    batchId = Guid.NewGuid().ToString()
                }
            }
        }
    };

    return JsonSerializer.Serialize(evt, new JsonSerializerOptions
    {
        PropertyNamingPolicy = null, // behold property names slik vi setter dem
        WriteIndented = false
    });
}


static async Task<bool> PostWithRetryAsync(
    HttpClient httpClient,
    string url,
    string json,
    int maxRetries,
    CancellationToken ct)
{
    // Simple exponential backoff
    for (int attempt = 1; attempt <= maxRetries; attempt++)
    {
        try
        {
            using var content = new StringContent(json, Encoding.UTF8, "application/json");
            using var resp = await httpClient.PostAsync(url, content, ct);

            if (resp.IsSuccessStatusCode)
                return true;

            // If 4xx (except 429), no point retrying
            if ((int)resp.StatusCode >= 400 && (int)resp.StatusCode < 500 && resp.StatusCode != (HttpStatusCode)429)
            {
                var body = await resp.Content.ReadAsStringAsync(ct);
                Console.WriteLine($"[HTTP {(int)resp.StatusCode}] Non-retryable. Body: {body}");
                return false;
            }

            var msg = await resp.Content.ReadAsStringAsync(ct);
            Console.WriteLine($"[WARN] HTTP {(int)resp.StatusCode} attempt {attempt}/{maxRetries}: {msg}");
        }
        catch (TaskCanceledException) when (!ct.IsCancellationRequested)
        {
            Console.WriteLine($"[WARN] Timeout on attempt {attempt}/{maxRetries}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[WARN] Exception attempt {attempt}/{maxRetries}: {ex.Message}");
        }

        // Backoff
        var delay = TimeSpan.FromSeconds(Math.Min(60, Math.Pow(2, attempt)));
        await Task.Delay(delay, ct);
    }

    return false;
}

// ------------------------------------------------------------
// RUN
// ------------------------------------------------------------
var scanned = 0;
var skipped = 0;
var posted = 0;
var failed = 0;

var sw = System.Diagnostics.Stopwatch.StartNew();

await foreach (var blob in container.GetBlobsAsync())
{
    scanned++;

    if (ShouldSkip(blob))
    {
        skipped++;
        continue;
    }

    var blobClient = container.GetBlobClient(blob.Name);
    var blobUrl = blobClient.Uri.ToString();

    var payload = MakeEventGridBlobCreatedPayload(blobUrl, "metervalues", blobUrl);

    var ok = await PostWithRetryAsync(http, ingestUrl, payload, maxRetries, CancellationToken.None);

    if (ok) posted++;
    else failed++;

    if ((posted + failed) % 250 == 0)
    {
        Console.WriteLine($"Progress: scanned={scanned:n0}, skipped={skipped:n0}, ok={posted:n0}, failed={failed:n0}, elapsed={sw.Elapsed}");
    }

    if (sendDelayMs > 0)
        await Task.Delay(sendDelayMs);

    if (maxBlobs > 0 && (posted + failed) >= maxBlobs)
        break;
}

sw.Stop();

Console.WriteLine();
Console.WriteLine("=== DONE ===");
Console.WriteLine($"Scanned: {scanned:n0}");
Console.WriteLine($"Skipped: {skipped:n0}");
Console.WriteLine($"OK:      {posted:n0}");
Console.WriteLine($"Failed:  {failed:n0}");
Console.WriteLine($"Time:    {sw.Elapsed}");