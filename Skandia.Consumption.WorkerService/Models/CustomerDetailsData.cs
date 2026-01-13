using Dapper;

namespace Skandia.Consumption.WorkerService.Models
{
    [Table("customerdetails", Schema = "elkompis")]
    public class CustomerDetailsData
    {
        [Key]
        [Column("id")]
        public int Id { get; set; }
        [Column("created")]
        public DateTime Created { get; set; } = DateTime.UtcNow;
        [Column("aduserid")]
        public string AdUserId { get; set; }
        [Column("customerid")]
        public int CustomerId { get; set; }
        [Column("firstname")]
        public string FirstName { get; set; }
        [Column("lastname")]
        public string LastName { get; set; }
        [Column("referralcode")]
        public string ReferralCode { get; set; }
        [Column("numofreferred")]
        public int NumOfReferred { get; set; }
        [Column("referralreward")]
        public decimal ReferralReward { get; set; }
        [Column("unpaidinvoices")]
        public int UnpaidInvoices { get; set; }
        [Column("daysuntilactivation")]
        public int? DaysUntilActivation { get; set; }
        [Column("consumptionpercentage")]
        public decimal? ConsumptionPercentage { get; set; }
        [Column("paymentmethodset")]
        public bool PaymentmethodSet { get; set; }
        [Column("householdprofileset")]
        public bool HouseholdProfileSet { get; set; }
        [Column("billingaddressconfirmed")]
        public bool BillingAddressConfirmed { get; set; }
        [Column("deliveries")]
        public string Deliveries { get; set; }
        [Column("enodeid")]
        public string EnodeId { get; set; }
        [Column("preferredname")]
        public string PreferredName { get; set; }
        [Column("language")]
        public string Language { get; set; }
        [Column("numofunreadnotifications")]
        public int NumOfUnreadNotifications { get; set; }
        [Column("numofunreadchatmessages")]
        public short NumOfUnreadChatMessages { get; set; }
        [Column("numofdaystonextinvoice")]
        public int? NumOfDaysToNextInvoice { get; set; }
        [Column("mobilenumber")]
        public string MobileNumber { get; set; }
        [Column("email")]
        public string Email { get; set; }
        [Column("intercomexternalid")]
        public string IntercomExternalId { get; set; }
        [Column("hvacuserid")]
        public string HvacUserId { get; set; }
        [Column("chargeruserid")]
        public string ChargerUserId { get; set; }
        [Column("appopening")]
        public DateTime? AppOpening { get; set; }

    }


    public class HighestConsumption
    {
        public int Type { get; set; }
        public double Energy { get; set; }
        public double Cost { get; set; }
        public DateTime Date { get; set; }
    }

    public class Product
    {
        public int Id { get; set; }
        public int ProductId { get; set; }
        public string Name { get; set; }
        public int Type { get; set; }
        public DateTime? StartDate { get; set; }
        public DateTime? EndDate { get; set; }
        public double Price { get; set; }
        public int Status { get; set; }
    }

    public class Delivery
    {
        public int DeliveryId { get; set; }
        public string Address { get; set; }
        public string StreetName { get; set; }
        public string HouseNumber { get; set; }
        public string City { get; set; }
        public string PostCode { get; set; }
        public string BillingStreetName { get; set; }
        public string BillingHouseNumber { get; set; }
        public string BillingCity { get; set; }
        public string BillingPostCode { get; set; }
        public string Mpid { get; set; }
        public List<Product> Products { get; set; }
        public int Status { get; set; }
        public int PriceArea { get; set; }
        public double? Latitude { get; set; }
        public double? Longitude { get; set; }
        public int ConsumptionPercentage { get; set; }
        public bool HouseholdProfileSet { get; set; }
        public bool BillingAddressConfirmed { get; set; }
        public string EnodeLocationId { get; set; }
        public string DisplayName { get; set; }
        public double TotalConsumption { get; set; }
        public string HouseType { get; set; }
        public string PaymentMethod { get; set; }
        public DateTime? StartDate { get; set; }
        public DateTime? EndDate { get; set; }
        public string TerminationReason { get; set; }
        public bool ViewedReactivation { get; set; }
        public List<HighestConsumption> HighestConsumption { get; set; }
        public string ConsumptionThisMonthCost { get; set; }
        public string ConsumptionThisMonthKwh { get; set; }
        public string ConsumptionYesterdayCost { get; set; }
        public string ConsumptionYesterdayKwh { get; set; }
        public string ConsumptionBreakdown { get; set; }
        public string ConsumptionSimilarHomes { get; set; }
        public string InvoiceLastMonthCost { get; set; }
        public string InvoiceThisMonthForecast { get; set; }
        public string InvoiceExpectedFunding { get; set; }
        public double? NorgesPrisTotal { get; set; }
    }



}

