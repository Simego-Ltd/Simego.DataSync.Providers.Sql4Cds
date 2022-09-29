using MarkMpn.Sql4Cds.Engine;
using System;
using System.Collections.Generic;
using System.Data;

namespace Simego.DataSync.Providers.Sql4Cds
{
    [ProviderInfo(Name = "Sql4Cds", Description = "Sql4Cds Description", Group = "SQL")]
    public class Sql4CdsDatasourceReader : DataReadOnlyReaderProviderBase
    {
        public string ConnectionString { get; set; }
        public string Command { get; set; } = "SELECT * FROM account";
       
        public override DataTableStore GetDataTable(DataTableStore dt)
        {
            DataSchemaMapping mapping = new DataSchemaMapping(SchemaMap, Side);
            var includedColumns = SchemaMap.GetIncludedColumns();

            using (var con = new Sql4CdsConnection(ConnectionString))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = Command;
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        dt.Rows.Add(mapping, includedColumns,
                           (item, columnName) =>
                           {
                               return reader[columnName];
                           });
                    }
                }
            }

            return dt;
        }
        
        public override DataSchema GetDefaultDataSchema()
        {
            var schema = new DataSchema();
            
            using (var connection = new Sql4CdsConnection(ConnectionString))
            using (var cmd = connection.CreateCommand())
            {                
                cmd.CommandText = Command;
                using (var reader = cmd.ExecuteReader())
                {
                    var schemaTable = reader.GetSchemaTable();

                    foreach (DataRow column in schemaTable.Rows)
                    {
                        schema.Map.Add(new DataSchemaItem((string)column["ColumnName"], (Type)column["DataType"], false, false, true, -1));
                    }                    
                }
            }

            return schema;
        }

        public override List<ProviderParameter> GetInitializationParameters()
        {
            return new List<ProviderParameter>
            {
                new ProviderParameter("ConnectionString", ConnectionString),
                new ProviderParameter("Command", Command)
            };
        }

        public override void Initialize(List<ProviderParameter> parameters)
        {
            foreach (ProviderParameter p in parameters)
            {                
                switch (p.Name)
                {
                    case "ConnectionString":
                        {
                            ConnectionString = p.Value;
                            break;
                        }
                    case "Command":
                        {
                            Command = p.Value;
                            break;
                        }
                    default:
                        {
                            break;
                        }
                }
            }
        }
    }
}
