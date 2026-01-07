using Microsoft.AspNetCore.Http;
using Skandia.Consumption.Ingest.Services;
using System.Text.Json;

namespace Skandia.Consumption.Ingest.Endpoints;

public static class MessageEndpoints
{
    public static void MapMessageEndpoints(this WebApplication app)
    {
        app.MapPost("/api/eventgrid/ingest", HandleEventGridEvent)
           .WithName("EventGridIngest");


        app.MapGet("/api/hello", () => Results.Text("Hello there", "text/plain"))
           .WithName("Hello");
    }

    private static async Task<IResult> HandleEventGridEvent(
        HttpRequest request,
        BlobIngestService ingestService,
        ILoggerFactory loggerFactory)
    {
        var logger = loggerFactory.CreateLogger("EventGrid");

        using var reader = new StreamReader(request.Body);
        var body = await reader.ReadToEndAsync();

        if (string.IsNullOrWhiteSpace(body))
        {
            logger.LogWarning("Empty Event Grid payload");
            return Results.BadRequest();
        }

        using var json = JsonDocument.Parse(body);

        foreach (var evt in json.RootElement.EnumerateArray())
        {
            var eventType = evt.GetProperty("eventType").GetString();

            // 1️⃣ Subscription validation (MÅ være med)
            if (eventType == "Microsoft.EventGrid.SubscriptionValidationEvent")
            {
                var validationCode = evt
                    .GetProperty("data")
                    .GetProperty("validationCode")
                    .GetString();

                logger.LogInformation("EventGrid subscription validation");

                return Results.Ok(new
                {
                    validationResponse = validationCode
                });
            }

            // 2️⃣ Blob created
            if (eventType == "Microsoft.Storage.BlobCreated")
            {
                if (!evt.TryGetProperty("data", out var data) ||
                    !data.TryGetProperty("url", out var urlProp))
                {
                    logger.LogError("BlobCreated event missing data.url");
                    return Results.Ok();
                }

                var blobUrl = urlProp.GetString()?.Trim();

                if (!Uri.TryCreate(blobUrl, UriKind.Absolute, out var blobUri))
                {
                    logger.LogError("Invalid blob URL from Event Grid: '{BlobUrl}'", blobUrl);
                    return Results.Ok();
                }
                await ingestService.ProcessAsync(blobUri);
            }
        }

        return Results.Ok();
    }
}
