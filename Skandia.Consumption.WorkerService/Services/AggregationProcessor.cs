using Microsoft.Extensions.Logging;
using Npgsql;
using Skandia.Consumption.Shared.Models;
using Skandia.Consumption.WorkerService.Models;
using Skandia.Consumption.WorkerService.Repositories;
using System.Diagnostics;
using System.Text.Json;

namespace Skandia.Consumption.WorkerService.Services;

public sealed class AggregationProcessor
{
    private const long SlowDbThresholdMs = 500;

    private readonly ILogger<AggregationProcessor> _logger;
    private readonly DataStorage _dataStorage;
    private readonly NpgsqlDataSource _dataSource;
    private readonly InvalidQueueService _invalidQueue;

    public AggregationProcessor(
            DataStorage dataStorage,
            NpgsqlDataSource dataSource,
            InvalidQueueService invalidQueue, 
            ILogger<AggregationProcessor> logger)
    {
        _dataStorage = dataStorage;
        _dataSource = dataSource;
        _logger = logger;
        _invalidQueue = invalidQueue;
    }

    public async Task ProcessAsync(
        AggregationMessage message)
    {
        var totalSw = Stopwatch.StartNew();

        var openSw = Stopwatch.StartNew();
        await using var conn = await _dataSource.OpenConnectionAsync();
        openSw.Stop();

        var deliverySw = Stopwatch.StartNew();
        var deliveryId = await _dataStorage.GetDeliveryId(conn, message.Mpid);
        deliverySw.Stop();

        if (deliveryId > 0)
        {
            var dbSw = Stopwatch.StartNew();

            try
            {
                var success = await _dataStorage.InsertAggregates(
                    conn,
                    deliveryId,
                    message.Source,
                    message.FromHour,
                    message.ToHour);

                if (!success)
                {
                    await _invalidQueue.SendAsync(new InvalidAggregationMessage
                    {
                        OriginalMessage = message,
                        Reason = "Contract not found",
                        Details = "No contract found for MPID {Mpid}",
                        FunctionName = "ProcessAsync"
                    });
                }


                //CustomerDetails dashboardupdates
                var customerDetails = await _dataStorage.GetCustomerDetails(conn, deliveryId);

                if (customerDetails != null)
                {
                    var deliveries = JsonSerializer.Deserialize<List<Delivery>>(customerDetails.Deliveries);

                    if (deliveries == null || deliveries.Count == 0)
                    {
                        _logger.LogWarning(
                            "No deliveries found in customer details for DeliveryId {DeliveryId}",
                            deliveryId);
                        return;
                    }

                    foreach (var d in deliveries)
                    {
                        var consumptionCostThisMonth = await _dataStorage.GetConsumptionData(conn, d.DeliveryId, ResolutionType.Month, new DateTime(DateTime.Today.Year, DateTime.Today.Month, 1), DateTime.Today.AddDays(-1), d.PriceArea);
                        var consumptionCostYesterday = await _dataStorage.GetConsumptionData(conn, d.DeliveryId, ResolutionType.Day, DateTime.Today.AddDays(-1), DateTime.Today.AddDays(-1), d.PriceArea);

                        if (consumptionCostThisMonth.Any())
                        {
                            d.ConsumptionThisMonthCost = Math.Round(consumptionCostThisMonth.Single().Cost / 100, 0).ToString();
                            d.ConsumptionThisMonthKwh = Math.Round(consumptionCostThisMonth.Single().Energy, 0).ToString();
                        }
                        if (consumptionCostYesterday.Any())
                        {
                            d.ConsumptionYesterdayCost = Math.Round(consumptionCostYesterday.Single().Cost / 100, 0).ToString();
                            d.ConsumptionYesterdayKwh = Math.Round(consumptionCostYesterday.Single().Energy, 0).ToString();
                        }


                        //HighestConsumption
                        var existingHighestConsumption = d.HighestConsumption;
                        var newHighestMonth = new List<HighestConsumption>();
                       
                        var current = new DateTime(message.FromHour.Year, message.FromHour.Month, 1);
                        var endMonth = new DateTime(message.ToHour.Year, message.ToHour.Month, 1);

                        while (current <= endMonth)
                        {
                            var monthStart = current;

                            var monthHighest = await _dataStorage.GetHighest3ConsumptionHours(conn, d.DeliveryId, monthStart);

                            newHighestMonth.AddRange(monthHighest.Select(c => new HighestConsumption
                            {
                                Date = c.Date,
                                Cost = c.Cost > 1000 ? Math.Round(c.Cost / 100, 0) : Math.Round(c.Cost / 100, 2),
                                Energy = c.Energy > 10 ? Math.Round(c.Energy, 0) : Math.Round(c.Energy, 2)
                            }));

                            current = current.AddMonths(1);
                        }

                        if (existingHighestConsumption != null)
                        {
                            foreach (var h in newHighestMonth)
                            {
                                var toBeRemoved = existingHighestConsumption.Where(e => e.Date.Year == h.Date.Year && e.Date.Month == h.Date.Month).ToList();
                                foreach (var r in toBeRemoved)
                                    existingHighestConsumption.Remove(r);
                            }
                        }

                        if (existingHighestConsumption == null && newHighestMonth.Count() > 0)
                            existingHighestConsumption = new List<HighestConsumption>();

                        foreach (var h in newHighestMonth)
                            existingHighestConsumption.Add(h);

                        d.HighestConsumption = existingHighestConsumption;

                    }
                    await _dataStorage.UpdateCustomerDetails(conn, deliveryId, JsonSerializer.Serialize(deliveries));
                }
                else
                {
                    _logger.LogWarning(
                        "No customer details found for DeliveryId {DeliveryId}",
                        deliveryId);
                }

            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "Error aggregating hours for MPID {Mpid}, DeliveryId {DeliveryId}, FromHour {FromHour}, ToHour {ToHour}, Source {Source}",
                    message.Mpid,
                    deliveryId,
                    message.FromHour,
                    message.ToHour,
                    message.Source);

                throw;
            }
            finally
            {
                dbSw.Stop();
                totalSw.Stop();
            }

            //if (dbSw.ElapsedMilliseconds >= SlowDbThresholdMs)
            //{
            //    _logger.LogWarning(
            //        "Slow aggregation (MPID {Mpid}): open={OpenMs}ms, delivery={DeliveryMs}ms, db={DbMs}ms, total={TotalMs}ms",
            //        message.Mpid,
            //        openSw.ElapsedMilliseconds,
            //        deliverySw.ElapsedMilliseconds,
            //        dbSw.ElapsedMilliseconds,
            //        totalSw.ElapsedMilliseconds);
            //}
            //else
            //{
            //    _logger.LogDebug(
            //        "Agg timings (MPID {Mpid}): open={OpenMs}ms, delivery={DeliveryMs}ms, db={DbMs}ms, total={TotalMs}ms",
            //        message.Mpid,
            //        openSw.ElapsedMilliseconds,
            //        deliverySw.ElapsedMilliseconds,
            //        dbSw.ElapsedMilliseconds,
            //        totalSw.ElapsedMilliseconds);
            //}
        }
        else
        {
            totalSw.Stop();

            await _invalidQueue.SendAsync(new InvalidAggregationMessage
            {
                OriginalMessage = message,
                Reason = "DeliveryId not found",
                Details = "No delivery found for MPID {Mpid}",
                FunctionName = "ProcessAsync"
            });

        }
    }
}