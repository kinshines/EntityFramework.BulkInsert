using EntityFramework.BulkInsert.Helpers;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Entity.Spatial;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EntityFramework.BulkInsert.Providers
{
    public class SqlBulkUpdateProvider : ProviderBase<SqlConnection, SqlTransaction>
    {
        public SqlBulkUpdateProvider()
        {
            SetProviderIdentifier("System.Data.SqlClient.SqlConnection");
        }

        public override object GetSqlGeography(string wkt, int srid)
        {
#if EF6
            var geo = new DbGeographyWellKnownValue
            {
                WellKnownText = wkt,
                CoordinateSystemId = srid
            };

            return DbSpatialServices.Default.CreateProviderValue(geo);
#endif
#if EF5
            return DbGeography.FromText(wkt, srid);
#endif
        }

        public override object GetSqlGeometry(string wkt, int srid)
        {
#if EF6          
            var geo = new DbGeometryWellKnownValue
            {
                WellKnownText = wkt,
                CoordinateSystemId = srid
            };

            return DbSpatialServices.Default.CreateProviderValue(geo);
#endif
#if EF5
            return DbGeometry.FromText(wkt, srid);
#endif
        }

        public override void Run<T>(IEnumerable<T> entities, SqlTransaction transaction)
        {
            throw new NotImplementedException();
        }

        public override Task RunAsync<T>(IEnumerable<T> entities, SqlTransaction transaction)
        {
            throw new NotImplementedException();
        }

        protected override SqlConnection CreateConnection()
        {
            return new SqlConnection(ConnectionString);
        }

        private SqlBulkCopyOptions ToSqlBulkCopyOptions(BulkCopyOptions bulkCopyOptions)
        {
            return (SqlBulkCopyOptions)(int)bulkCopyOptions;
        }
    }
}
