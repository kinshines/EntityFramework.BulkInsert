using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Entity;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EntityFramework.BulkInsert.Extensions
{
    public static class BulkUpdateExtension
    {

#if NET45

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="context"></param>
        /// <param name="entities"></param>
        /// <param name="options"></param>
        public static Task BulkUpdateAsync<T>(this DbContext context, IEnumerable<T> entities, BulkInsertOptions options)
        {
            var bulkUpdate = UpdateProviderFactory.Get(context);
            bulkUpdate.Options = options;
            return bulkUpdate.RunAsync(entities);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="context"></param>
        /// <param name="entities"></param>
        /// <param name="batchSize"></param>
        /// <returns></returns>
        public static Task BulkUpdateAsync<T>(this DbContext context, IEnumerable<T> entities, int? batchSize = null)
        {
            return context.BulkUpdateAsync(entities, BulkCopyOptions.Default, batchSize);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="context"></param>
        /// <param name="entities"></param>
        /// <param name="bulkCopyOptions"></param>
        /// <param name="batchSize"></param>
        public static Task BulkUpdateAsync<T>(this DbContext context, IEnumerable<T> entities, BulkCopyOptions bulkCopyOptions, int? batchSize = null)
        {
            var options = new BulkInsertOptions { BulkCopyOptions = bulkCopyOptions };
            if (batchSize.HasValue)
            {
                options.BatchSize = batchSize.Value;
            }
            return context.BulkUpdateAsync(entities, options);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="context"></param>
        /// <param name="entities"></param>
        /// <param name="transaction"></param>
        /// <param name="bulkCopyOptions"></param>
        /// <param name="batchSize"></param>
        public static Task BulkUpdateAsync<T>(this DbContext context, IEnumerable<T> entities, IDbTransaction transaction, BulkCopyOptions bulkCopyOptions = BulkCopyOptions.Default, int? batchSize = null)
        {
            var options = new BulkInsertOptions { BulkCopyOptions = bulkCopyOptions };
            if (transaction != null)
            {
                options.Connection = transaction.Connection;
                options.Transaction = transaction;
            }
            if (batchSize.HasValue)
            {
                options.BatchSize = batchSize.Value;
            }
            return context.BulkInsertAsync(entities, options);
        }

#endif

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="context"></param>
        /// <param name="entities"></param>
        /// <param name="options"></param>
        public static void BulkUpdate<T>(this DbContext context, IEnumerable<T> entities, BulkInsertOptions options)
        {
            var bulkUpdate = UpdateProviderFactory.Get(context);
            bulkUpdate.Options = options;
            bulkUpdate.Run(entities);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="context"></param>
        /// <param name="entities"></param>
        /// <param name="batchSize"></param>
        public static void BulkUpdate<T>(this DbContext context, IEnumerable<T> entities, int? batchSize = null)
        {
            context.BulkUpdate(entities, BulkCopyOptions.Default, batchSize);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="context"></param>
        /// <param name="entities"></param>
        /// <param name="sqlBulkCopyOptions"></param>
        /// <param name="batchSize"></param>
        public static void BulkUpdate<T>(this DbContext context, IEnumerable<T> entities, BulkCopyOptions bulkCopyOptions, int? batchSize = null)
        {

            var options = new BulkInsertOptions { BulkCopyOptions = bulkCopyOptions };
            if (batchSize.HasValue)
            {
                options.BatchSize = batchSize.Value;
            }
            context.BulkUpdate(entities, options);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="context"></param>
        /// <param name="entities"></param>
        /// <param name="transaction"></param>
        /// <param name="bulkCopyOptions"></param>
        /// <param name="batchSize"></param>
        public static void BulkUpdate<T>(this DbContext context, IEnumerable<T> entities, IDbTransaction transaction, BulkCopyOptions bulkCopyOptions = BulkCopyOptions.Default, int? batchSize = null)
        {
            var options = new BulkInsertOptions { BulkCopyOptions = bulkCopyOptions };
            if (transaction != null)
            {
                options.Connection = transaction.Connection;
                options.Transaction = transaction;
            }

            if (batchSize.HasValue)
            {
                options.BatchSize = batchSize.Value;
            }
            context.BulkUpdate(entities, options);
        }
    }
}
