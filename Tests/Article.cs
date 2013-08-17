using System;
using Dapper;

namespace Tests 
{
	class Db : Database<Db>
	{
		public Table<Article> Article { get; set; }
		public Table<ArticleFile> ArticleFile { get; set; }
		public Table<User> Users { get; set; }
		public Table<UserRole> UserRole { get; set; }
	}

	public class User
	{
		public int Id { get; set; }
		public string Name { get; set; }
		public string Email { get; set; }
		public bool IsActive { get; set; }
		public DateTime? Created { get; set; }
		public DateTime? Changed { get; set; }
	}

	public class UserRole
	{
		public int UserId { get; set; }
		public int RoleId { get; set; }
	}

	public class Article
	{
		public int Id { get; set; }
		public string Name { get; set; }
		public bool Disabled { get; set; }
		public DateTime Created { get; set; }
	}

	public class ArticleFile
	{
		public int Id { get; set; }
		public byte[] File { get; set; }
	}
}
