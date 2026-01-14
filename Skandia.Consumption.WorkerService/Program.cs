using Azure.Identity;
using Azure.Storage.Queues;
using Microsoft.Azure.Functions.Worker.Builder;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Npgsql;
using Skandia.Consumption.WorkerService.Repositories;
using Skandia.Consumption.WorkerService.Services;


var builder = FunctionsApplication.CreateBuilder(args);

var keyVaultUri = Environment.GetEnvironmentVariable("VaultUri");
if (!string.IsNullOrWhiteSpace(keyVaultUri))
{
    builder.Configuration.AddAzureKeyVault(new Uri(keyVaultUri), new DefaultAzureCredential());    
}
builder.Services.AddSingleton<NpgsqlDataSource>(_ =>
{
    var connStr = builder.Configuration["db-crm-connectionstring-novalidation"];
    if (string.IsNullOrWhiteSpace(connStr))
        throw new InvalidOperationException("db-crm-connectionstring-novalidation is not set");

   var dataSourceBuilder = new NpgsqlDataSourceBuilder(connStr);
    return dataSourceBuilder.Build();
});

builder.Services.AddSingleton<InvalidQueueService>(sp =>
{
    var storageConn = builder.Configuration["AzureWebJobsBlobStorageUC"]; 

    if (string.IsNullOrWhiteSpace(storageConn))
        throw new InvalidOperationException("AzureWebJobsStorage is not set");

    var queueClient = new QueueClient(storageConn, "aggregation-invalid");
    return new InvalidQueueService(queueClient);
});

builder.Services.AddMemoryCache();

builder.Services.AddSingleton<DataStorage>();
builder.Services.AddSingleton<AggregationProcessor>();
builder.Services.AddSingleton<ICacheService<decimal>, MemoryCacheService<decimal>>();

var app = builder.Build();

app.Run();

