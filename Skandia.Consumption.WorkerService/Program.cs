using Azure.Identity;
using Azure.Storage.Queues;
using Skandia.Consumption.Shared.Models;
using Skandia.Consumption.WorkerService.Repositories;
using Skandia.Consumption.WorkerService.Services;
using Skandia.Consumption.WorkerService.Workers;
using Skandia.DB;

var builder = Host.CreateApplicationBuilder(args);

var keyVaultUri = Environment.GetEnvironmentVariable("VaultUri");
if (!string.IsNullOrWhiteSpace(keyVaultUri))
{
    builder.Configuration.AddAzureKeyVault(new Uri(keyVaultUri), new DefaultAzureCredential());
}

builder.Services.AddSingleton(_ =>
{
    var connectionString =
        builder.Configuration["blobstorageuc-connectionstring"];

    var client = new QueueClient(
        connectionString,
        "aggregation-queue");

    client.CreateIfNotExists();
    return client;
});

builder.Services.AddTransient<IUnitOfWork, UnitOfWork>();
builder.Services.AddTransient<IRepository<MeterValueData>, Repository<MeterValueData>>();
builder.Services.AddTransient<DataStorage>();
builder.Services.AddScoped<AggregationProcessor>();
builder.Services.AddHostedService<AggregationQueueWorker>();

var host = builder.Build();
host.Run();
