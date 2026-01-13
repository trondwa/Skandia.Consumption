using Dapper;
using Microsoft.Extensions.Caching.Memory;
using Npgsql;
using Skandia.Consumption.WorkerService.Models;
using Skandia.Consumption.WorkerService.Repositories;
using Skandia.Consumption.WorkerService.Services;
using Testcontainers.PostgreSql;

namespace Skandia.Consumption.Test;

[TestFixture]
public sealed class DataStorageGetConsumptionDataTests
{
    private PostgreSqlContainer _db = default!;
    private NpgsqlConnection _conn = default!;
    private DataStorage _sut = default!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _db = new PostgreSqlBuilder()
            .WithImage("postgres:16-alpine")
            .WithDatabase("skandia_test")
            .WithUsername("postgres")
            .WithPassword("postgres")
            .Build();

        await _db.StartAsync();

        _conn = new NpgsqlConnection(_db.GetConnectionString());
        await _conn.OpenAsync();

        await CreateSchemaAsync(_conn);
        await SeedAsync(_conn);

        _sut = new DataStorage(new FakeDecimalCacheService());
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _conn.DisposeAsync();
        await _db.DisposeAsync();
    }

    [Test]
    public async Task GetConsumptionData_Hour_ReturnsRowsWithDate()
    {
        var from = new DateTime(2026, 01, 01, 10, 30, 00);
        var to = new DateTime(2026, 01, 01, 12, 15, 00);

        var items = await _sut.GetConsumptionData(
            _conn,
            deliveryId: 1,
            resolution: ResolutionType.Hour,
            from: from,
            to: to,
            productId: 10,
            priceArea: null,
            actualOnly: false);

        Assert.That(items, Has.Count.EqualTo(3));

        Assert.That(items[0].Period, Is.EqualTo(10));
        Assert.That(items[0].Energy, Is.EqualTo(1.1m));
        Assert.That(items[0].Cost, Is.EqualTo(11m));
        Assert.That(items[0].Date, Is.EqualTo(new DateTime(2026, 01, 01, 10, 00, 00)));

        Assert.That(items[1].Period, Is.EqualTo(11));
        Assert.That(items[2].Period, Is.EqualTo(12));
    }

    [Test]
    public async Task GetConsumptionData_Day_GroupsByDay_AndDateIsNull()
    {
        var from = new DateTime(2026, 01, 01, 00, 00, 00);
        var to = new DateTime(2026, 01, 02, 00, 00, 00);

        var items = await _sut.GetConsumptionData(
            _conn,
            deliveryId: 1,
            resolution: ResolutionType.Day,
            from: from,
            to: to,
            productId: 10,
            priceArea: null,
            actualOnly: false);

        Assert.That(items, Has.Count.EqualTo(2));

        var day1 = items.Single(x => x.Period == 1);
        Assert.That(day1.Energy, Is.EqualTo(3.0m));
        Assert.That(day1.Cost, Is.EqualTo(30m));
        Assert.That(day1.Date, Is.Null);

        var day2 = items.Single(x => x.Period == 2);
        Assert.That(day2.Energy, Is.EqualTo(5.0m));
        Assert.That(day2.Cost, Is.EqualTo(50m));
        Assert.That(day2.Date, Is.Null);
    }

    [Test]
    public async Task GetConsumptionData_Month_AddsMonthlyPrice_WhenPriceAreaIsProvided()
    {
        var from = new DateTime(2026, 01, 01, 00, 00, 00);
        var to = new DateTime(2026, 01, 31, 00, 00, 00);

        var items = await _sut.GetConsumptionData(
            _conn,
            deliveryId: 1,
            resolution: ResolutionType.Month,
            from: from,
            to: to,
            productId: 10,
            priceArea: 1,
            actualOnly: false);

        Assert.That(items, Has.Count.EqualTo(1));

        // monthly_aggregates.cost = 800
        // productlist.PriceMonth = 1.23 -> * 100 => +123
        Assert.That(items[0].Period, Is.EqualTo(1));
        Assert.That(items[0].Energy, Is.EqualTo(100m));
        Assert.That(items[0].Cost, Is.EqualTo(923m));
        Assert.That(items[0].Date, Is.Null);
    }

    [Test]
    public async Task GetConsumptionData_ActualOnly_FiltersOutNonActualRows()
    {
        var from = new DateTime(2026, 01, 02, 00, 00, 00);
        var to = new DateTime(2026, 01, 02, 00, 00, 00);

        var items = await _sut.GetConsumptionData(
            _conn,
            deliveryId: 1,
            resolution: ResolutionType.Day,
            from: from,
            to: to,
            productId: 10,
            priceArea: null,
            actualOnly: true);

        // In seed: day 2 is actual=false, so it should be filtered out
        Assert.That(items, Is.Empty);
    }

    private static async Task CreateSchemaAsync(NpgsqlConnection conn)
    {
        var sql = """
                  CREATE SCHEMA IF NOT EXISTS consumption;
                  CREATE SCHEMA IF NOT EXISTS register;

                  DROP TABLE IF EXISTS consumption.hour_aggregates;
                  DROP TABLE IF EXISTS consumption.daily_aggregates;
                  DROP TABLE IF EXISTS consumption.monthly_aggregates;
                  DROP TABLE IF EXISTS register.productlist;

                  CREATE TABLE consumption.hour_aggregates (
                      deliveryid  INT NOT NULL,
                      mpid        TEXT NOT NULL,
                      date        TIMESTAMP NOT NULL,
                      consumption NUMERIC NOT NULL,
                      cost        NUMERIC NOT NULL,
                      actual      BOOLEAN NOT NULL
                  );

                  CREATE TABLE consumption.daily_aggregates (
                      deliveryid  INT NOT NULL,
                      date        TIMESTAMP NOT NULL,
                      consumption NUMERIC NOT NULL,
                      cost        NUMERIC NOT NULL,
                      actual      BOOLEAN NOT NULL
                  );

                  CREATE TABLE consumption.monthly_aggregates (
                      deliveryid  INT NOT NULL,
                      date        TIMESTAMP NOT NULL,
                      consumption NUMERIC NOT NULL,
                      cost        NUMERIC NOT NULL,
                      actual      BOOLEAN NOT NULL
                  );

                  CREATE TABLE register.productlist (
                      id          INT PRIMARY KEY,
                      pricemonth  NUMERIC NULL
                  );
                  """;

        await conn.ExecuteAsync(sql);
    }

    private static async Task SeedAsync(NpgsqlConnection conn)
    {
        await conn.ExecuteAsync(
            "INSERT INTO register.productlist (id, pricemonth) VALUES (@id, @pricemonth);",
            new { id = 10, pricemonth = 1.23m });

        await conn.ExecuteAsync(
            """
            INSERT INTO consumption.hour_aggregates (deliveryid, mpid, date, consumption, cost, actual)
            VALUES
                (1, 'mp1', '2026-01-01 10:00:00', 1.1, 11, true),
                (1, 'mp1', '2026-01-01 11:00:00', 0.9, 9,  true),
                (1, 'mp1', '2026-01-01 12:00:00', 1.0, 10, true);
            """);

        await conn.ExecuteAsync(
            """
            INSERT INTO consumption.daily_aggregates (deliveryid, date, consumption, cost, actual)
            VALUES
                (1, '2026-01-01 00:00:00', 3.0, 30, true),
                (1, '2026-01-02 00:00:00', 5.0, 50, false);
            """);

        await conn.ExecuteAsync(
            """
            INSERT INTO consumption.monthly_aggregates (deliveryid, date, consumption, cost, actual)
            VALUES
                (1, '2026-01-01 00:00:00', 100, 800, true);
            """);
    }

    private sealed class FakeDecimalCacheService : ICacheService<decimal>
    {
        private readonly Dictionary<string, decimal> _cache = new(StringComparer.Ordinal);

        public Task<decimal> Get(string key)
            => Task.FromResult(_cache.TryGetValue(key, out var v) ? v : 0m);

        public void Set(string key, decimal entry, MemoryCacheEntryOptions options = null)
            => _cache[key] = entry;

        public void Remove(string key)
            => _cache.Remove(key);
    }
}
