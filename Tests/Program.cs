using System;
using Dapper;
using Npgsql;

namespace Tests
{
	class Program
	{
		public static Db Db {
			get {
				var cn = new NpgsqlConnection ("Server=127.0.0.1;Port=5432;Database=postgres;User Id=anton;MaxPoolSize=100;");
				cn.Open ();
				return Db.Init (cn, 30);
			}
		}

		static void Performance ()
		{
			const int iterations = 20;
			var test = new PerformanceTests ();
			Console.WriteLine ("Running {0} iterations that load up a post entity", iterations);
			test.Run (iterations);
			Console.WriteLine ("Finish...");
		}

		public static void Main (string[] args)
		{
            var x = Db.Users.Insert (new { Name = "Anton" });
            Db.Users.Update(x, new { Name = "Anton test" });
			Console.WriteLine (x);
		}

		static void TestBlob()
		{
			var id = 5;
			var file = new System.Drawing.Bitmap(@"/Users/anton/personal/image/foto-ktm.jpg");
			using (var m = new System.IO.MemoryStream()) {
				file.Save (m, System.Drawing.Imaging.ImageFormat.Jpeg);
				Db.ArticleFile.InsertOrUpdate (id, new { File = m.GetBuffer() });
			}
			var a = Db.ArticleFile.Get (id);
			using (var m = new System.IO.MemoryStream(a.File)) {
				var i = new System.Drawing.Bitmap (m);
				i.Save ("foto-ktm.jpg");
			}
			Db.Dispose ();
		}
	}
}
