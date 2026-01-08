using Dapper;
using System.Reflection;

namespace Skandia.Consumption.Shared.Helpers
{
    public static class Storage
    {
        public static string GetBulkString(Type type)
        {
            var tableAttribute = (TableAttribute)Attribute.GetCustomAttribute(type, typeof(TableAttribute));
            var tableName = tableAttribute.Name;
            var schema = tableAttribute.Schema;

            var sql = $"COPY {schema}.{tableName} (";

            PropertyInfo[] properties = type.GetProperties();
            foreach (PropertyInfo property in properties)
            {
                if (property.Name.ToLower() != "id")
                    sql += property.Name.ToLower() + ",";
            }

            sql = sql.TrimEnd(',');

            sql += ") FROM STDIN (FORMAT BINARY)";

            return sql;
        }
    }
}
