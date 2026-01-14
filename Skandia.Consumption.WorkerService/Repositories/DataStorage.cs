using Dapper;
using Microsoft.Extensions.Caching.Memory;
using Npgsql;
using Skandia.Consumption.WorkerService.Models;
using Skandia.Consumption.WorkerService.Services;

namespace Skandia.Consumption.WorkerService.Repositories;

public class DataStorage
{
    private readonly ICacheService<decimal> _memoryCacheService;

    public DataStorage(ICacheService<decimal> memoryCacheService)
    {
        _memoryCacheService = memoryCacheService;
    }

    public async Task<int> GetDeliveryId(NpgsqlConnection conn, string mpid, DateTime toDate)
    {
        var sql = "SELECT max(id) FROM data.delivery d WHERE mpid = @mpid AND d.status = 2;";
        var deliveryId = await conn.ExecuteScalarAsync<int?>(sql, new { mpid });

        if (deliveryId.HasValue)
            return deliveryId.Value;

        sql = "SELECT max(id) FROM data.delivery d WHERE mpid = @mpid AND d.status in (3,33) AND d.enddate >= @toDate";
        deliveryId = await conn.ExecuteScalarAsync<int?>(sql, new { mpid, toDate });

        if (deliveryId.HasValue)
            return deliveryId.Value;

        sql = "SELECT max(id) FROM data.delivery d WHERE mpid = @mpid AND d.status = 13 AND d.startdate < current_date;";
        deliveryId = await conn.ExecuteScalarAsync<int?>(sql, new { mpid });

        if (deliveryId.HasValue)
            return deliveryId.Value;

        return 0;

    }


    public async Task<bool> InsertAggregates(NpgsqlConnection conn, int deliveryId, string source, DateTime fromHour, DateTime toHour)
    {
        var sql = @"
            WITH delivery AS (
                SELECT
                    d.id,
                    d.mpid,
                    d.pricearea,
                    CASE WHEN d.pricearea <> 4 THEN 1.25 ELSE 1 END AS tax,
                    (
                        SELECT COALESCE(pl.pricekwh, 0)
                        FROM data.contract c
                        INNER JOIN register.productlist pl ON pl.id = c.productid
                        WHERE
                            c.deliveryid = d.id
                            AND c.enddate IS NULL
                            AND pl.pricekwhno1 IS NULL
                            AND pl.addon = FALSE
                        ORDER BY c.startdate DESC
                        LIMIT 1
                    ) AS margin
                FROM data.delivery d
                WHERE d.id = @deliveryId
            ),
            prices AS (
                SELECT
                    date_trunc('hour', p.starttime) AS hour,
                    AVG(p.value) AS valueprice
                FROM data.nordpoolprices p
                CROSS JOIN delivery d
                WHERE
                    p.pricearea = 'NO' || d.pricearea
                    AND p.starttime >= @fromHour
                    AND p.starttime <= @toHour
                GROUP BY date_trunc('hour', p.starttime)
            ),
            consumption AS (
                SELECT
                    rd.hour,
                    SUM(rd.value) AS valueconsumption
                FROM consumption.raw_data rd
                WHERE
                    rd.source = @source
                    AND rd.direction = 'Out'
                    AND rd.hour >= @fromHour
                    AND rd.hour <= @toHour
                GROUP BY rd.hour
            ),
            base AS (
                SELECT
                    now() AS created,
                    d.id AS deliveryid,
                    d.mpid,
                    c.hour AS date,
                    (p.valueprice + d.margin) * d.tax AS price,
                    c.valueconsumption AS consumption,
                    ((p.valueprice + d.margin) * d.tax * c.valueconsumption) AS cost,
                    TRUE AS actual
                FROM delivery d
                INNER JOIN consumption c ON c.hour IS NOT NULL
                INNER JOIN prices p ON p.hour = c.hour
            )
            INSERT INTO consumption.hour_aggregates (
                created,
                deliveryid,
                mpid,
                date,
                price,
                consumption,
                cost,
                actual
            )
            SELECT created, deliveryid, mpid, date, price, consumption, cost, actual
            FROM base
            ON CONFLICT (deliveryid, date)
            DO UPDATE SET
                created     = EXCLUDED.created,
                price       = EXCLUDED.price,
                consumption = EXCLUDED.consumption,
                cost        = EXCLUDED.cost,
                actual      = EXCLUDED.actual;

            INSERT INTO consumption.daily_aggregates (
                deliveryid,
                mpid,
                date,
                consumption,
                cost,
                actual,
                created
            )
            SELECT
                ha.deliveryid,
                ha.mpid,
                DATE(ha.date)       AS date,
                SUM(ha.consumption) AS consumption,
                SUM(ha.cost)        AS cost,
                BOOL_AND(ha.actual) AS actual,
                now()
            FROM consumption.hour_aggregates ha
            WHERE ha.deliveryid = @deliveryId
              AND ha.date >= @fromHour
              AND ha.date <= @toHour
            GROUP BY
                ha.deliveryid,
                ha.mpid,
                DATE(ha.date)
            ON CONFLICT (deliveryid, date)
            DO UPDATE SET
                mpid        = EXCLUDED.mpid,
                consumption = EXCLUDED.consumption,
                cost        = EXCLUDED.cost,
                actual      = EXCLUDED.actual,
                created     = now();

            INSERT INTO consumption.monthly_aggregates (
                deliveryid,
                date,
                consumption,
                cost,
                actual,
                created
            )
            SELECT
                da.deliveryid,
                date_trunc('month', da.date)::date AS date,
                SUM(da.consumption)               AS consumption,
                SUM(da.cost)                      AS cost,
                BOOL_AND(da.actual)               AS actual,
                now()
            FROM consumption.daily_aggregates da
            WHERE da.deliveryid = @deliveryId
              AND da.date >= @fromHour
              AND da.date <= @toHour
            GROUP BY
                da.deliveryid,
                date_trunc('month', da.date)
            ON CONFLICT (deliveryid, date)
            DO UPDATE SET
                consumption = EXCLUDED.consumption,
                cost        = EXCLUDED.cost,
                actual      = EXCLUDED.actual,
                created     = now();
            ";

        try
        {
            await conn.ExecuteAsync(sql, new
            {
                deliveryId,
                source,
                fromHour,
                toHour
            });
            return true;
        }
        catch (Exception ex)
        {

            return false;
        }
    }

    public async Task<CustomerDetailsData?> GetCustomerDetails(NpgsqlConnection conn, int deliveryId)
    {
        var sql = "SELECT c.* FROM elkompis.customerdetails c INNER JOIN data.delivery d ON c.customerid = d.customerid WHERE d.id = @deliveryId;";
        var customerDetails = await conn.QueryAsync<CustomerDetailsData>(sql, new { deliveryId });

        if (customerDetails != null)
            return customerDetails.FirstOrDefault();

        return null;
    }

    public async Task<List<ConsumptionItem>> GetConsumptionData(NpgsqlConnection conn, int deliveryId, ResolutionType resolution, DateTime from, DateTime to, int productId, short? priceArea = null, bool actualOnly = false)
    {
        var sql = string.Empty;
        var fromDate = new DateTime(from.Year, from.Month, from.Day, 0, 0, 0);
        var toDate = new DateTime(to.Year, to.Month, to.Day, 23, 0, 0);

        decimal monthlyPrice = 0;

        if (priceArea.HasValue)
            monthlyPrice = await GetProductMonthlyPrice(conn, productId) * 100;

        var actualyOnlySql = string.Empty;

        if (actualOnly)
            actualyOnlySql = " AND actual = true";

        switch (resolution)
        {
            case ResolutionType.Hour:
                sql = $"select date_part('hour', date) as Period, ROUND(CAST(consumption as numeric),18) as Energy, CAST(cost as real) as Cost, date as Date from consumption.hour_aggregates where deliveryid = @deliveryId {actualyOnlySql} and date between TO_TIMESTAMP(@dateFrom, 'YYYY-MM-DD HH24:00:00') and TO_TIMESTAMP(@dateTo, 'YYYY-MM-DD HH24:00:00') order by date";
                break;
            case ResolutionType.Day:
                sql = $"select date_part('day', date) as Period, ROUND(CAST(sum(consumption) as numeric),18) as Energy, CAST(sum(cost) as real) as Cost, null as Date from consumption.daily_aggregates where deliveryid = @deliveryId {actualyOnlySql} and date between TO_TIMESTAMP(@dateFrom, 'YYYY-MM-DD HH24:00:00') and TO_TIMESTAMP(@dateTo, 'YYYY-MM-DD HH24:00:00') group by date_part('day', date) order by date_part('day', date)";
                break;
            case ResolutionType.Month:
                sql = $"select date_part('month', date) as Period, ROUND(CAST(sum(consumption) as numeric),18) as Energy, CAST(sum(cost) as real) + {monthlyPrice.ToString().Replace(",", ".")} as Cost, null as Date from consumption.monthly_aggregates where deliveryid = @deliveryId {actualyOnlySql} and date between TO_TIMESTAMP(@dateFrom, 'YYYY-MM-DD HH24:00:00') and TO_TIMESTAMP(@dateTo, 'YYYY-MM-DD HH24:00:00') group by date_part('month', date) order by date_part('month', date)";
                break;
            default:
                break;
        }

        var consumptionItems = await conn.QueryAsync<ConsumptionItem>(sql, new { deliveryId, dateFrom = fromDate.ToString("yyyy-MM-dd HH:mm"), dateTo = toDate.ToString("yyyy-MM-dd HH:mm") });

        return consumptionItems.ToList();

    }

    public async Task<List<HighestConsumption>> GetHighest3ConsumptionHours(NpgsqlConnection conn, int deliveryId, DateTime from)
    {
        var sql = string.Empty;
        var fromDate = new DateTime(from.Year, from.Month, from.Day, 0, 0, 0);
        var toDate = fromDate.AddMonths(1).AddHours(-1);

        sql = $@"WITH ranked_per_day AS (
                        SELECT
                            ha.consumption as energy,
                            ha.cost as cost,
                            ha.date as date,
                            row_number() OVER (
                                PARTITION BY date_trunc('day', ha.date)
                                ORDER BY ha.consumption DESC, ha.date ASC
                            ) AS rn
                        FROM consumption.hour_aggregates ha
                        WHERE ha.deliveryid = @deliveryId
                          AND ha.date >= @fromDate
                          AND ha.date <= @toDate
                    )
                    SELECT
                        energy,
                        cost::double precision,
                        date
                    FROM ranked_per_day
                    WHERE rn = 1              
                    ORDER BY energy DESC
                    LIMIT 3;";


        var highestConsumption = await conn.QueryAsync<HighestConsumption>(sql, new { deliveryId, fromDate = fromDate, toDate = toDate });

        return highestConsumption.ToList();
    }

    public async Task UpdateCustomerDetails(NpgsqlConnection conn, int deliveryId, string deliveries)
    {
        var sql = @"
            UPDATE elkompis.customerdetails
            SET
                deliveries               = @deliveries,
                created                  = @created
            WHERE customerid = (SELECT customerid FROM data.delivery WHERE id = @deliveryId);";

        await conn.ExecuteAsync(sql, new
        {
            deliveries,
            deliveryId,
            created = DateTime.UtcNow
        });
    }

    private async Task<decimal> GetProductMonthlyPrice(NpgsqlConnection conn, int productId)
    {
        var cacheKey = $"ProductPricePrMonth[{productId}]";

        var pricePrMonth = await _memoryCacheService.Get(cacheKey);

        if (pricePrMonth > 0)
            return pricePrMonth;

        var sql = "SELECT * FROM register.productlist where id = @productId;";
        var productPrice = (await conn.QueryAsync<dynamic>(sql, new { productId })).FirstOrDefault();


        if (productPrice == null)
        {
            return 0;
        }

        pricePrMonth = productPrice.PriceMonth ?? 0;

        if (pricePrMonth > 0)
        {
            var options = new MemoryCacheEntryOptions()
            {
                SlidingExpiration = TimeSpan.FromDays(1)
            };

            _memoryCacheService.Set(cacheKey, pricePrMonth, options);
            return pricePrMonth;
        }
        else
            return 0;
    }


}
