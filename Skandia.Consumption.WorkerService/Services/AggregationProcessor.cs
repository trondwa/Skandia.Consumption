using Skandia.Consumption.Shared.Models;
using Skandia.Consumption.WorkerService.Models;
using Skandia.Consumption.WorkerService.Repositories;
using System;
using System.Collections.Generic;
using System.Text;

namespace Skandia.Consumption.WorkerService.Services;

public sealed class AggregationProcessor
{
    private readonly ILogger<AggregationProcessor> _logger;
    private readonly DataStorage _dataStorage;

    public AggregationProcessor(
            DataStorage dataStorage,
            ILogger<AggregationProcessor> logger)
    {
        _dataStorage = dataStorage;
        _logger = logger;
    }

    public async Task ProcessAsync(
        AggregationMessage message,
        CancellationToken ct)
    {
        var meterValues = await _dataStorage.GetMeterValuesAsync(message.Source);

        var deleteMessage = true;
        var deliveryId = await _dataStorage.GetDeliveryId(message.Mpid);

        if (deliveryId > 0)
        {
            // HOUR AGGREGATES

            var prices = await _dataStorage.GetPrices(
                                deliveryId,
                                message.FromHour,
                                message.ToHour);

            foreach (var item in meterValues.Where(m => m.Direction == "Out"))
            {
                var price = prices.FirstOrDefault(p => p.Hour == item.Hour);
                if (price != null)
                {
                    var totalPrice = (price.Price + price.Margin ?? 0) * price.Tax;
                    var agg = new HourAggregate
                    {
                        Created = DateTime.UtcNow,
                        DeliveryId = deliveryId,
                        Mpid = item.Mpid,
                        Date = item.Hour,
                        Price = totalPrice,
                        Consumption = item.Value,
                        Cost = totalPrice * item.Value,
                        Actual = true
                    };

                    await _dataStorage.InsertHourAggregates(agg);
                }
                else
                {
                    _logger.LogWarning("No price found for MPID: {Mpid} at Hour: {Hour}", item.Mpid, item.Hour);
                    deleteMessage = false;
                }
            }

            // DAILY AGGREGATES
            await _dataStorage.InsertDailyAggregates(
                                                    deliveryId,
                                                    message.FromHour,
                                                    message.ToHour);


            // MONTHLY AGGREGATES
            await _dataStorage.InsertMonthlyAggregates(
                                                    deliveryId,
                                                    message.FromHour,
                                                    message.ToHour);

            // CUSTOMER DETAILS
            
            //TODO:Implementer dette

        }
        else
        {
            _logger.LogWarning("No delivery found for MPID: {Mpid}", message.Mpid);
            deleteMessage = false;
        }




        await Task.CompletedTask;
    }

}



