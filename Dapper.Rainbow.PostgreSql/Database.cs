/*
 License: http://www.apache.org/licenses/LICENSE-2.0 
 Home page: http://code.google.com/p/dapper-dot-net/
*/

using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Data;
using Dapper;
using System.Collections.Concurrent;
using System.Reflection;
using System.Text;
using System.Diagnostics;
using System.Reflection.Emit;
using System.Text.RegularExpressions;

namespace Dapper
{
    /// <summary>
    /// A container for a database, assumes all the tables have an Id column named Id
    /// </summary>
    /// <typeparam name="TDatabase"></typeparam>
    public abstract class Database<TDatabase> : IDisposable where TDatabase : Database<TDatabase>, new()
    {
        public class Table<T, TId>
        {
            internal Database<TDatabase> database;
            internal string tableName;
            internal string likelyTableName;
			internal string schema = "public";

            public Table(Database<TDatabase> database, string likelyTableName)
            {
                this.database = database;
                this.likelyTableName = likelyTableName;
            }

            public string TableName
            {
                get
                {
                    tableName = tableName ?? database.DetermineTableName<T>(likelyTableName);
                    return schema + "." + tableName;
                }
            }

            /// <summary>
            /// Insert a row into the db
            /// </summary>
            /// <param name="data">Either DynamicParameters or an anonymous type or concrete type</param>
            /// <returns></returns>
            public virtual TId Insert(dynamic data)
            {
                var o = (object)data;
                var paramNames = GetParamNames(o);

                string cols = string.Join(",", paramNames);
                string cols_params = string.Join(",", paramNames.Select(p => "@" + p));
				var sql = "INSERT INTO " + TableName + " (" + cols + ") VALUES (" + cols_params + ") RETURNING id";

				return database.Query<TId>(sql, o).FirstOrDefault();
			}

            /// <summary>
            /// Update a record in the DB
            /// </summary>
            /// <param name="id"></param>
            /// <param name="data"></param>
            /// <returns></returns>
            public int Update(TId id, dynamic data)
            {
                return Update(new { id }, data);
            }

            public int Update(dynamic where, dynamic data)
            {
                var paramNames = GetParamNames((object)data);
				var keys = GetParamNames((object)where);

                var b = new StringBuilder();
                b.Append("UPDATE ").Append(TableName).Append(" SET ");
                b.AppendLine(string.Join(",", paramNames.Select(p => p + " = @" + p)));
				b.Append(" WHERE ").Append(string.Join(" AND ", keys.Select(p => p + " = @" + p)));

                var parameters = new DynamicParameters(data);
                parameters.AddDynamicParams(where);
                return database.Execute(b.ToString(), parameters);
            }

            /// <summary>
            /// Insert a row into the db or update when key is duplicated 
            /// only for autoincrement key
            /// </summary>
			/// <param name="id"></param>
            /// <param name="data">Either DynamicParameters or an anonymous type or concrete type</param>
            /// <returns></returns>
            public TId InsertOrUpdate(TId id, dynamic data)
            {
                return InsertOrUpdate(new { id }, data);
            }

			/// <summary>
			/// Insert a row into the db or update when key is duplicated 
			/// for autoincrement key
			/// </summary>
			/// <param name="where">Where clause</param>
			/// <param name="data">Either DynamicParameters or an anonymous type or concrete type</param>
			/// <returns></returns>
            public TId InsertOrUpdate(dynamic key, dynamic data)
            {   
                var paramNames = GetParamNames((object)data);
                var k = GetParamNames((object)key).Single();
                
                string cols = string.Join(",", paramNames);
                string cols_params = string.Join(",", paramNames.Select(p => "@" + p));
                string cols_update = string.Join(",", paramNames.Select(p => p + " = @" + p)); 
				var parameters = new DynamicParameters(data);
				parameters.AddDynamicParams(key);
				var b = new StringBuilder();
				try {
					b.Append("INSERT INTO ").Append(TableName).Append("(")
				 	 .Append(cols).Append(",").Append(k).Append(") VALUES (").Append(cols_params).Append(", @").Append(k)
				 	 .Append(") RETURNING id");
					return database.Query<TId>(b.ToString(), parameters).Single();
				} catch { 
					b.Clear ().Append("UPDATE ").Append(TableName)
					 .Append(" SET ").Append(cols_update)
					 .Append(" WHERE ").Append(k).Append(" = @").Append(k)
					 .Append(" RETURNING id");
					return database.Query<TId>(b.ToString(), parameters).Single();
				}
			}

            /// <summary>
            /// Delete a record for the DB
            /// </summary>
            /// <param name="id"></param>
            /// <returns></returns>
            public bool Delete(TId id)
            {
                return database.Execute("DELETE FROM " + TableName  + " WHERE Id = @id", new { id }) > 0;
            }

			/// <summary>
			/// Delete a record for the DB
			/// </summary>
			/// <param name="where"></param>
			/// <returns></returns>
            public bool Delete(dynamic where = null)
            {
                if (where == null) return database.Execute("TRUNCATE " + TableName) > 0;
				var o = (object)where;
                var paramNames = GetParamNames(o);
                var w = string.Join(" AND ", paramNames.Select(p => p + " = @" + p));
                return database.Execute("DELETE FROM " + TableName + " WHERE " + w, o) > 0;
            }

            /// <summary>
            /// Grab a record with a particular Id from the DB 
            /// </summary>
            /// <param name="id"></param>
            /// <returns></returns>
            public T Get(TId id)
            {
                return database.Query<T>("SELECT * FROM " + TableName + " WHERE id = @id", new { id }).FirstOrDefault();
            }

            public T Get(dynamic where)
            {
				var o = (object)where;
				var paramNames = GetParamNames(o);
				var w = string.Join(" AND ", paramNames.Select(p => p + " = @" + p));
				return database.Query<T>("SELECT * FROM " + TableName + " WHERE " + w, o).FirstOrDefault();
            }

            public T First(dynamic where = null)
            {
                if (where == null) return database.Query<T>("SELECT * FROM " + TableName + " LIMIT 1").FirstOrDefault();
				var o = (object)where;
                var paramNames = GetParamNames(o);
                var w = string.Join(" AND ", paramNames.Select(p => p + " = @" + p));
                return database.Query<T>("SELECT * FROM "+ TableName + " WHERE " + w + " LIMIT 1", o).FirstOrDefault();
            }

			public List<T> All()
			{
				return database.Query<T>("SELECT * FROM " + TableName).ToList();
			}

            static ConcurrentDictionary<Type, List<string>> paramNameCache = new ConcurrentDictionary<Type, List<string>>();

            internal static List<string> GetParamNames(object o)
            {
                if (o is DynamicParameters)
                {
					return (o as DynamicParameters).ParameterNames.ToList();
                }

                List<string> paramNames;
                if (!paramNameCache.TryGetValue(o.GetType(), out paramNames))
                {
                    paramNames = new List<string>();
                    foreach (var prop in o.GetType().GetProperties(BindingFlags.GetProperty | BindingFlags.Instance | BindingFlags.Public))
                    {
						paramNames.Add(prop.Name);
                    }
                    paramNameCache[o.GetType()] = paramNames;
                }
                return paramNames;
            }
        }

		public class Table<T> : Table<T, int> {
			public Table(Database<TDatabase> database, string likelyTableName)
				: base (database, likelyTableName)
			{
			}
		}

        IDbConnection connection;
        int commandTimeout;
        IDbTransaction transaction;


        public static TDatabase Init(IDbConnection connection, int commandTimeout)
        {
            TDatabase db = new TDatabase();
            db.InitDatabase(connection, commandTimeout);
            return db;
        }

        internal static Action<TDatabase> tableConstructor;

        internal void InitDatabase(IDbConnection connection, int commandTimeout)
        {
            this.connection = connection;
            this.commandTimeout = commandTimeout;
            if (tableConstructor == null)
            {
				tableConstructor = CreateTableConstructorForTable();
            }

            tableConstructor(this as TDatabase);
        }

		internal virtual Action<TDatabase> CreateTableConstructorForTable ()
		{
			return CreateTableConstructor(typeof(Table<>));
		}

        public void BeginTransaction(IsolationLevel isolation = IsolationLevel.ReadCommitted)
        {
            transaction = connection.BeginTransaction(isolation);
        }

        public void CommitTransaction()
        {
            transaction.Commit();
            transaction = null;
        }

        public void RollbackTransaction()
        {
            transaction.Rollback();
            transaction = null;
        }

		protected Action<TDatabase> CreateTableConstructor(Type tableType)
        {
            var dm = new DynamicMethod("ConstructInstances", null, new Type[] { typeof(TDatabase) }, true);
            var il = dm.GetILGenerator();

            var setters = GetType().GetProperties()
                .Where(p => p.PropertyType.IsGenericType && p.PropertyType.GetGenericTypeDefinition() == tableType)
                .Select(p => Tuple.Create(
                        p.GetSetMethod(true),
                        p.PropertyType.GetConstructor(new Type[] { typeof(TDatabase), typeof(string) }),
                        p.Name,
                        p.DeclaringType
                 ));

            foreach (var setter in setters)
            {
                il.Emit(OpCodes.Ldarg_0);
                // [db]

                il.Emit(OpCodes.Ldstr, setter.Item3);
                // [db, likelyname]

                il.Emit(OpCodes.Newobj, setter.Item2);
                // [table]

                var table = il.DeclareLocal(setter.Item2.DeclaringType);
                il.Emit(OpCodes.Stloc, table);
                // []

                il.Emit(OpCodes.Ldarg_0);
                // [db]

                il.Emit(OpCodes.Castclass, setter.Item4);
                // [db cast to container]

                il.Emit(OpCodes.Ldloc, table);
                // [db cast to container, table]

                il.Emit(OpCodes.Callvirt, setter.Item1);
                // []
            }

            il.Emit(OpCodes.Ret);
            return (Action<TDatabase>)dm.CreateDelegate(typeof(Action<TDatabase>));
        }

        static ConcurrentDictionary<Type, string> tableNameMap = new ConcurrentDictionary<Type, string>();
        private string DetermineTableName<T>(string likelyTableName)
        {
            string name;

            if (!tableNameMap.TryGetValue(typeof(T), out name))
            {
				name = likelyTableName.ToUnderScore ();
                if (!TableExists(name))
                {
					name = typeof(T).Name.ToUnderScore();
                }

                tableNameMap[typeof(T)] = name;
            }
            return name;
        }

        private bool TableExists(string name)
        {
            return connection.Query("select 1 from INFORMATION_SCHEMA.TABLES where TABLE_NAME = @name", 
                new { name }, transaction: transaction).Count() == 1;
        }

        public int Execute(string sql, dynamic param = null)
        {
            return SqlMapper.Execute(connection, sql, param as object, transaction, commandTimeout: this.commandTimeout);
        }

        public IEnumerable<T> Query<T>(string sql, dynamic param = null, bool buffered = true)
        {
            
            return SqlMapper.Query<T>(connection, sql, param as object, transaction, buffered, commandTimeout);
        }

        public IEnumerable<TReturn> Query<TFirst, TSecond, TReturn>(string sql, Func<TFirst, TSecond, TReturn> map, dynamic param = null, IDbTransaction transaction = null, bool buffered = true, string splitOn = "Id", int? commandTimeout = null)
        {
            return SqlMapper.Query(connection, sql, map, param as object, transaction, buffered, splitOn);
        }

        public IEnumerable<TReturn> Query<TFirst, TSecond, TThird, TReturn>(string sql, Func<TFirst, TSecond, TThird, TReturn> map, dynamic param = null, IDbTransaction transaction = null, bool buffered = true, string splitOn = "Id", int? commandTimeout = null)
        {
            return SqlMapper.Query(connection, sql, map, param as object, transaction, buffered, splitOn);
        }

        public IEnumerable<TReturn> Query<TFirst, TSecond, TThird, TFourth, TReturn>(string sql, Func<TFirst, TSecond, TThird, TFourth, TReturn> map, dynamic param = null, IDbTransaction transaction = null, bool buffered = true, string splitOn = "Id", int? commandTimeout = null)
        {
            return SqlMapper.Query(connection, sql, map, param as object, transaction, buffered, splitOn);
        }

        public IEnumerable<TReturn> Query<TFirst, TSecond, TThird, TFourth, TFifth, TReturn>(string sql, Func<TFirst, TSecond, TThird, TFourth, TFifth, TReturn> map, dynamic param = null, IDbTransaction transaction = null, bool buffered = true, string splitOn = "Id", int? commandTimeout = null)
        {
            return SqlMapper.Query(connection, sql, map, param as object, transaction, buffered, splitOn);
        }

        public IEnumerable<dynamic> Query(string sql, dynamic param = null, bool buffered = true)
        {
            return SqlMapper.Query(connection, sql, param as object, transaction, buffered);
        }

        public Dapper.SqlMapper.GridReader QueryMultiple(string sql, dynamic param = null, IDbTransaction transaction = null, int? commandTimeout = null, CommandType? commandType = null)
        {
            return SqlMapper.QueryMultiple(connection, sql, param, transaction, commandTimeout, commandType);
        }


        public void Dispose()
        {
            if (connection == null) return;
            if (connection.State != ConnectionState.Closed)
            {
                if (transaction != null)
                {
                    transaction.Rollback();
                }

                connection.Close();
                connection = null;
            }
        }
    }

	public static class Extensions
	{
		public static string ToUnderScore(this string text)
		{
			var sb = new StringBuilder (text.Length);
			sb.Append (Char.ToLowerInvariant (text [0]));
			foreach (var t in text.Substring (1)) {
				if (Char.IsLower (t) || t == '_') sb.Append (t);
				else sb.Append ("_").Append (Char.ToLowerInvariant (t));
			}
			return sb.ToString();
		}
	}
}