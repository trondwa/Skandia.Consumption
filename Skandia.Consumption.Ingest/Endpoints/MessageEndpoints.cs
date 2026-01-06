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
                var data = evt.GetProperty("data");

                var blobUrl = data.GetProperty("url").GetString();
                var contentType = data.GetProperty("contentType").GetString();

                logger.LogInformation("Blob created: {BlobUrl}", blobUrl);

                await ingestService.ProcessAsync(blobUrl);
            }
        }

        return Results.Ok();
    }
}
