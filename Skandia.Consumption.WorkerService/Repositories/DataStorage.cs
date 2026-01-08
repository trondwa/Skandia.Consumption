using Dapper;
using Skandia.Consumption.Shared.Models;
using Skandia.Consumption.WorkerService.Models;
using Skandia.DB;

namespace Skandia.Consumption.WorkerService.Repositories;

public class DataStorage
{
    private readonly IRepository<MeterValueData> _meterValueRepository;

    public DataStorage(IRepository<MeterValueData> meterValueRepository)
    {
        _meterValueRepository = meterValueRepository;
    }

    public async Task<IEnumerable<MeterValueData>> GetMeterValuesAsync(string source)
    {
        return await _meterValueRepository.GetAsync("WHERE source = @source",
            new { source });
    }

    public async Task<int> GetDeliveryId(string mpid)
    {
        var conn = _meterValueRepository.UnitOfWork.GetConnection();
        var sql = "SELECT max(id) FROM data.delivery d WHERE mpid = @mpid AND d.status = 2;";
        var deliveryId = await conn.ExecuteScalarAsync<int?>(sql, new { mpid });

        if (deliveryId.HasValue)
            return deliveryId.Value;

        sql = "SELECT max(id) FROM data.delivery d WHERE mpid = @mpid AND d.status = 3 AND d.enddate > current_date - interval '30' day;";
        deliveryId = await conn.ExecuteScalarAsync<int?>(sql, new { mpid });

        if (deliveryId.HasValue)
            return deliveryId.Value;

        sql = "SELECT max(id) FROM data.delivery d WHERE mpid = @mpid AND d.status = 13 AND d.startdate < current_date;";
        deliveryId = await conn.ExecuteScalarAsync<int?>(sql, new { mpid });

        if (deliveryId.HasValue)
            return deliveryId.Value;

        return 0;

    }


    public async Task<IEnumerable<PriceData>> GetPrices(int deliveryId, DateTime fromHour, DateTime toHour)
    {
        var conn = _meterValueRepository.UnitOfWork.GetConnection();
        var sql = @"select
	                    d.id as deliveryid,
                        date_trunc('hour', starttime) AS hour,
                        AVG(value)                     AS price,
                        (select coalesce(pricekwh,0) from register.productlist p inner join data.contract c on c.productid = p.id where p.pricekwhno1 is null and c.deliveryid = d.id and c.enddate is null and p.addon = false order by c.startdate desc limit 1) as margin,
                        COALESCE((select 1.25 where d.pricearea <> 4), 1) as tax
                    FROM data.nordpoolprices p, data.delivery d 
                    where d.id = @deliveryId 
	                    AND p.pricearea = 'NO' || d.pricearea
	                    AND p.starttime >= @fromHour
	                    AND p.starttime <=  @toHour
                    GROUP BY d.id, hour
                    ORDER BY hour;";

        var prices = await conn.QueryAsync<PriceData>(sql, new { deliveryId, fromHour, toHour });

        return prices;
    }


    public async Task InsertHourAggregates(HourAggregate hourAggregate)
    {
        var conn = _meterValueRepository.UnitOfWork.GetConnection();
        var sql = @"INSERT INTO consumption.hour_aggregates (
                    created,
                    deliveryid,
                    mpid,
                    date,
                    price,
                    consumption,
                    cost,
                    actual
                )
                VALUES (
                    @created,
                    @deliveryid,
                    @mpid,
                    @date,
                    @price  ,
                    @consumption,
                    @cost,
                    @actual
                )
                ON CONFLICT (mpid, date)
                DO UPDATE SET
                    created     = EXCLUDED.created;
                    price  = EXCLUDED.price,
                    consumption = EXCLUDED.consumption,
                    cost        = EXCLUDED.cost,
                    actual      = EXCLUDED.actual
                ";

        await conn.ExecuteAsync(sql, new { hourAggregate.Created, 
                                           hourAggregate.DeliveryId, 
                                           hourAggregate.Mpid, 
                                           hourAggregate.Date, 
                                           hourAggregate.Price, 
                                           hourAggregate.Consumption, 
                                           hourAggregate.Cost, 
                                           hourAggregate.Actual });
    }


    public async Task InsertDailyAggregates(int deliveryId, DateTime fromHour, DateTime toHour)
    {
        var conn = _meterValueRepository.UnitOfWork.GetConnection();
        var sql = @"INSERT INTO consumption.daily_aggregates (
                        deliveryid,
                        mpid,
                        day,
                        consumption,
                        cost,
                        actual,
                        created
                    )
                    SELECT
                        ha.deliveryid,
                        ha.mpid,
                        DATE(ha.hour)           AS day,
                        SUM(ha.consumption)     AS consumption,
                        SUM(ha.cost)            AS cost,
                        BOOL_AND(ha.actual)     AS actual,
                        now()
                    FROM consumption.hour_aggregates ha
                    WHERE ha.deliveryid = @deliveryId
                      AND ha.hour >= @fromHour
                      AND ha.hour <  @toHour
                    GROUP BY
                        ha.deliveryid,
                        ha.mpid,
                        DATE(ha.hour)
                    ON CONFLICT (deliveryid, day)
                    DO UPDATE SET
                        mpid        = EXCLUDED.mpid,
                        consumption = EXCLUDED.consumption,
                        cost        = EXCLUDED.cost,
                        actual      = EXCLUDED.actual,
                        created     = now();
                ";

        await conn.ExecuteAsync(sql, new
        {
            deliveryId,
            fromHour,
            toHour
        });
    }

}
