using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Skandia.Consumption.Shared.Models;
using Skandia.Consumption.WorkerService.Services;

namespace Skandia.Consumption.WorkerService;

public class ConsumptionAggregationFunction
{

    private readonly AggregationProcessor _aggregationProcessor; 
    private readonly ILogger<ConsumptionAggregationFunction> _logger;

    public ConsumptionAggregationFunction(ILogger<ConsumptionAggregationFunction> logger, AggregationProcessor aggregationProcessor)
    {
        _logger = logger;
        _aggregationProcessor = aggregationProcessor;
    }

    [Function("ConsumptionAggregationFunction")]
    public async Task RunAsync([QueueTrigger("aggregation-queue", Connection = "BlobStorageUC")] AggregationMessage message)
    {
        _logger.LogInformation(
                 "Processing aggregation message for MPID {Mpid} from {From} to {To}",
                 message.Mpid,
                 message.FromHour,
                 message.ToHour);

                await _aggregationProcessor.ProcessAsync(message);
    }
}