using Azure.Identity;
using Azure.Storage.Blobs;
using Skandia.Consumption.Ingest.Endpoints;
using Skandia.Consumption.Ingest.Services;

var builder = WebApplication.CreateBuilder(args);

var keyVaultUri = Environment.GetEnvironmentVariable("VaultUri");
if (!string.IsNullOrWhiteSpace(keyVaultUri))
{
    builder.Configuration.AddAzureKeyVault(new Uri(keyVaultUri), new DefaultAzureCredential());
}

// Services
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddLogging();

builder.Services.AddSingleton(_ =>
    new BlobServiceClient(builder.Configuration["blobstorageuc-connectionstring"]));

builder.Services.AddScoped<BlobIngestService>();

var app = builder.Build();

// Map endpoints
app.MapMessageEndpoints();

app.Run();
