﻿using System.ComponentModel.DataAnnotations;

namespace Thinktecture.IdentityServer.Repositories.Sql
{
    public class Client
    {
        [Key]
        public int Id { get; set; }

        [Required]
        public string Name { get; set; }
        
        [Required]
        public string Description { get; set; }

        [Required]
        public string ClientId { get; set; }

        public string ClientSecret { get; set; }

        public string RedirectUri { get; set; }

        public bool AllowRefreshToken { get; set; }

        public bool AllowImplicitFlow { get; set; }

        public bool AllowResourceOwnerFlow { get; set; }

        public bool AllowCodeFlow { get; set; }
    }
}