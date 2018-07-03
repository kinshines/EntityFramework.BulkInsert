using EntityFramework.BulkInsert.Helpers;
using EntityFramework.BulkInsert.Providers;
using MySql.Data.MySqlClient;
using MySql.Data.Types;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EntityFramework.BulkInsert.MySql
{
    public class MySqlBulkUpdateProvider : ProviderBase<MySqlConnection, MySqlTransaction>
    {
        public MySqlBulkUpdateProvider()
        {
            SetProviderIdentifier("MySql.Data.MySqlClient.MySqlConnection");
        }

        public override object GetSqlGeography(string wkt, int srid)
        {
            throw new NotImplementedException();
        }

        public override object GetSqlGeometry(string wkt, int srid)
        {
            if (!MySqlGeometry.TryParse(wkt, out MySqlGeometry value))
                return null;
            return value;
        }

        public override void Run<T>(IEnumerable<T> entities, MySqlTransaction transaction)
        {
            Run(entities, transaction.Connection, transaction);
        }

        public override Task RunAsync<T>(IEnumerable<T> entities, MySqlTransaction transaction)
        {
            return RunAsync(entities, transaction.Connection, transaction);
        }

        protected override MySqlConnection CreateConnection()
        {
            return new MySqlConnection(ConnectionString);
        }
        protected override string ConnectionString => DbConnection.ConnectionString;

        private void Run<T>(IEnumerable<T> entities, MySqlConnection connection, MySqlTransaction transaction)
        {
            if (!entities.Any())
                return;

            bool keepIdentity = (BulkCopyOptions.KeepIdentity & Options.BulkCopyOptions) > 0;
            bool keepNulls = (BulkCopyOptions.KeepNulls & Options.BulkCopyOptions) > 0;

            using (var reader = new MappedDataReader<T>(entities, this))
            {
                var tableEngine = GetTableEngine(connection.Database, reader.TableName, connection);

                var keyColumns = reader.Cols
                    .Where(x => !x.Value.Computed && x.Value.IsPk)
                    .ToArray();
                var columns = reader.Cols
                    .Where(x => !x.Value.Computed && !x.Value.IsPk && (!x.Value.IsIdentity || keepIdentity))
                    .ToArray();

                // UPDATE [TableName] (column list)
                var updateHeader = $"UPDATE `{reader.TableName}` SET ";
                var updateBuilder = new StringBuilder();

                int i = 0;
                long rowsCopied = 0;
                while (reader.Read())
                {
                    updateBuilder.Append(updateHeader);
                    foreach (var col in columns)
                    {
                        var value = reader.GetValue(col.Key);
                        var type = col.Value.Type;
                        updateBuilder.AppendFormat("{0}={1},", col.Value.ColumnName, AddParameter(type, value));
                    }
                    updateBuilder.Remove(updateBuilder.Length - 1, 1);//remove last ,
                    updateBuilder.Append(" WHERE ");
                    foreach(var col in keyColumns)
                    {
                        var value = reader.GetValue(col.Key);
                        var type = col.Value.Type;
                        updateBuilder.AppendFormat("{0}={1} AND ", col.Value.ColumnName, AddParameter(type, value));
                    }
                    updateBuilder.Remove(updateBuilder.Length - 5, 5);//remove last AND
                    updateBuilder.Append(";");

                    i++;

                    if (i == Options.BatchSize || i == Options.NotifyAfter)
                    {
                        using (var cmd = CreateCommand(CreateUpdateBatchText(updateBuilder, tableEngine), connection, transaction))
                            cmd.ExecuteNonQueryAsync();

                        if (Options.Callback != null)
                        {
                            int batches = Options.BatchSize / Options.NotifyAfter;

                            rowsCopied += i;
                            Options.Callback(this, new RowsCopiedEventArgs(rowsCopied));
                        }
                        i = 0;
                    }
                }

                if (i>0)
                {
                    using (var cmd = CreateCommand(CreateUpdateBatchText(updateBuilder, tableEngine), connection, transaction))
                        cmd.ExecuteNonQuery();
                }
            }
        }

#if NET45
        private async Task RunAsync<T>(IEnumerable<T> entities, MySqlConnection connection, MySqlTransaction transaction)
        {
            if (!entities.Any())
                return ;

            bool keepIdentity = (BulkCopyOptions.KeepIdentity & Options.BulkCopyOptions) > 0;
            bool keepNulls = (BulkCopyOptions.KeepNulls & Options.BulkCopyOptions) > 0;

            using (var reader = new MappedDataReader<T>(entities, this))
            {
                var tableEngine = await GetTableEngineAsync(connection.Database, reader.TableName, connection);

                var keyColumns = reader.Cols
                    .Where(x => !x.Value.Computed && x.Value.IsPk)
                    .ToArray();
                var columns = reader.Cols
                    .Where(x => !x.Value.Computed && !x.Value.IsPk && (!x.Value.IsIdentity || keepIdentity))
                    .ToArray();

                // UPDATE [TableName] (column list)
                var updateHeader = $"UPDATE `{reader.TableName}` SET ";
                var updateBuilder = new StringBuilder();

                int i = 0;
                long rowsCopied = 0;

                while (reader.Read())
                {
                    updateBuilder.Append(updateHeader);
                    foreach (var col in columns)
                    {
                        var value = reader.GetValue(col.Key);
                        var type = col.Value.Type;
                        updateBuilder.AppendFormat("{0}={1},", col.Value.ColumnName, AddParameter(type, value));
                    }
                    updateBuilder.Remove(updateBuilder.Length - 1, 1);//remove last ,
                    updateBuilder.Append(" WHERE ");
                    foreach (var col in keyColumns)
                    {
                        var value = reader.GetValue(col.Key);
                        var type = col.Value.Type;
                        updateBuilder.AppendFormat("{0}={1} AND ", col.Value.ColumnName, AddParameter(type, value));
                    }
                    updateBuilder.Remove(updateBuilder.Length - 5, 5);//remove last AND
                    updateBuilder.Append(";");

                    i++;

                    if (i == Options.BatchSize || i == Options.NotifyAfter)
                    {
                        using (var cmd = CreateCommand(CreateUpdateBatchText(updateBuilder, tableEngine), connection, transaction))
                            await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);

                        if (Options.Callback != null)
                        {
                            int batches = Options.BatchSize / Options.NotifyAfter;

                            rowsCopied += i;
                            Options.Callback(this, new RowsCopiedEventArgs(rowsCopied));
                        }

                        i = 0;
                    }
                }

                if (i>0)
                {
                    using (var cmd = CreateCommand(CreateUpdateBatchText(updateBuilder, tableEngine), connection, transaction))
                        await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
                }
            }
        }
#endif

        private string AddParameter(Type type,object value)
        {
            if (type == null
                || type == typeof(string)
                || type == typeof(Guid?)
                || type == typeof(Guid))
            {
                if (value == null)
                {
                    return "NULL";
                }
                else
                {
                    return $"'{MySqlHelper.EscapeString(value.ToString())}'";
                }
            }

            if (IsDateType(type))
            {
                if (value == null)
                {
                    return "NULL";
                }
                else
                {
                    const string dateTimePattern = "yyyy-MM-dd HH:mm:ss.ffffff";
                    if (value is DateTime dt)
                    {
                        return $"'{MySqlHelper.EscapeString(dt.ToString(dateTimePattern))}'";
                    }
                    else if (value is DateTimeOffset dt2)
                    {
                        return $"'{MySqlHelper.EscapeString(dt2.ToString(dateTimePattern))}'";
                    }
                }
            }

            if (type.IsEnum)
            {
                if (value == null)
                {
                    return "NULL";
                }
                else
                {
                    var enumUnderlyingType = type.GetEnumUnderlyingType();
                    return Convert.ChangeType(value, enumUnderlyingType).ToString();
                }
            }

            if (value == null)
            {
                return "NULL";
            }
            else
            {
                return value.ToString();
            }
        }

        private MySqlCommand CreateCommand(string commandText, MySqlConnection connection, MySqlTransaction transaction)
        {
            var cmd = new MySqlCommand(commandText, connection)
            {
                CommandTimeout = Options.TimeOut
            };

            if (transaction != null)
            {
                cmd.Transaction = transaction;
            }
            return cmd;
        }

        private string CreateUpdateBatchText(StringBuilder updateSql, MySqlEngine engine)
        {
            return GenerateSql(engine, Options.BulkCopyOptions, () => updateSql.ToString());
        }

        private MySqlEngine GetTableEngine(string schemaName, string tableName, MySqlConnection connection)
        {
            using (var cmd = GetTableEngineCommand(schemaName, tableName, connection))
            {
                var engine = cmd.ExecuteScalar();

                Enum.TryParse(engine.ToString(), true, out MySqlEngine tableEngine);

                return tableEngine;
            }
        }

        private async Task<MySqlEngine> GetTableEngineAsync(string schemaName, string tableName, MySqlConnection connection)
        {
            using (var cmd = GetTableEngineCommand(schemaName, tableName, connection))
            {
                var engine = await cmd.ExecuteScalarAsync().ConfigureAwait(false);

                Enum.TryParse(engine.ToString(), true, out MySqlEngine tableEngine);

                return tableEngine;
            }
        }

        private MySqlCommand GetTableEngineCommand(string schemaName, string tableName, MySqlConnection connection)
        {
            var commandText = $@"select engine 
                                from   information_schema.tables 
                                where  table_schema = '{schemaName}'
                                   and table_name = '{tableName}'";

            return CreateCommand(commandText, connection, null);
        }

        private bool IsDateType(Type type)
        {
            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>))
                return IsDateType(Nullable.GetUnderlyingType(type));

            return type == typeof(DateTime) || type == typeof(DateTimeOffset);
        }

        private string GenerateSql(MySqlEngine engine, BulkCopyOptions options, Func<string> statementGenerator)
        {
            switch (engine)
            {
                case MySqlEngine.InnoDB:
                    return GenerateInnoDbOptimizations(options, statementGenerator);
                case MySqlEngine.MyISAM:
                    return statementGenerator();
                default:
                    return statementGenerator();
            }
        }

        private string GenerateInnoDbOptimizations(BulkCopyOptions options, Func<string> statementGenerator)
        {
            bool checkConstraints = true;
            if (!options.HasFlag(BulkCopyOptions.Default))
            {
                checkConstraints = options.HasFlag(BulkCopyOptions.CheckConstraints);
            }

            if (!checkConstraints)
            {
                return $@"
SET autocommit=0;
SET unique_checks=0;
SET foreign_key_checks=0;
{statementGenerator()}
COMMIT;
SET unique_checks=1;
SET foreign_key_checks=1;
SET autocommit=1";
            }
            return $@"
SET autocommit=0;
{statementGenerator()}
COMMIT;
SET autocommit=1;";
        }
    }
}
