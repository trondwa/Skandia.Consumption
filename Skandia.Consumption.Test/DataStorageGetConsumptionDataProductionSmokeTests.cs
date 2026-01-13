using Dapper;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Configuration;
using Npgsql;
using Skandia.Consumption.WorkerService.Models;
using Skandia.Consumption.WorkerService.Repositories;
using Skandia.Consumption.WorkerService.Services;
using Azure.Identity;

namespace Skandia.Consumption.Test;

[TestFixture]
[Category("Production")]
public sealed class DataStorageGetConsumptionDataProductionSmokeTests
{
    private const string VaultUriEnv = "https://key-app-skandia-prod.vault.azure.net/";
    private const string EnvConnStr = "db-crm-connectionstring-novalidation";

    [Test]
    [Explicit($"Uses real database connection from environment variable '{EnvConnStr}' OR Azure Key Vault via '{VaultUriEnv}'.")]
    public async Task GetConsumptionData_OnRealDatabase_ReturnsSomeRows_ForRecentHours()
    {
        var connStr = await TryResolveConnectionStringAsync();
        if (string.IsNullOrWhiteSpace(connStr))
        {
            Assert.Ignore($"No connection string resolved. Set '{EnvConnStr}' or '{VaultUriEnv}'.");

            return;
        }

        await using var conn = new NpgsqlConnection(connStr);
        await conn.OpenAsync();

        var cache = new NullDecimalCacheService();
        var sut = new DataStorage(cache);

        var deliveryId = 20143; // await conn.ExecuteScalarAsync<int?>("select max(id) from data.delivery where status in (2,3,13);");

        //if (!deliveryId.HasValue || deliveryId.Value <= 0)
        //{
        //    Assert.Ignore("No delivery found in database. Skipping.");
        //    return;
        //}

        var from = new DateTime(2026, 1, 1);
        var to = new DateTime(2026, 1, 2);

        var items = await sut.GetConsumptionData(
            conn,
            deliveryId: deliveryId,
            resolution: ResolutionType.Hour,
            from: from,
            to: to,
            productId: 0,
            priceArea: null,
            actualOnly: false);

        Assert.That(items, Is.Not.Null);
        Assert.That(items, Is.Not.Empty);
    }

    private static async Task<string?> TryResolveConnectionStringAsync()
    {
        var viaEnv = Environment.GetEnvironmentVariable(EnvConnStr);
        if (!string.IsNullOrWhiteSpace(viaEnv))
            return viaEnv;

        var vaultUri = VaultUriEnv; // Environment.GetEnvironmentVariable(VaultUriEnv);
        if (string.IsNullOrWhiteSpace(vaultUri))
            return null;

        var config = new ConfigurationBuilder();

        config.AddAzureKeyVault(
            new Uri(vaultUri),
            new DefaultAzureCredential());

        var built = config.Build();

        var fromKv = built[EnvConnStr];
        if (!string.IsNullOrWhiteSpace(fromKv))
            return fromKv;

        // Fallback: some setups store secrets with -- instead of :
        fromKv = built[EnvConnStr.Replace("-", "--")];
        if (!string.IsNullOrWhiteSpace(fromKv))
            return fromKv;

        await Task.CompletedTask;
        return null;
    }

    private sealed class NullDecimalCacheService : ICacheService<decimal>
    {
        public Task<decimal> Get(string key) => Task.FromResult(0m);
        public void Set(string key, decimal entry, MemoryCacheEntryOptions options = null) { }
        public void Remove(string key) { }
    }
}
