using System;

namespace Yoda.AspNetCore.SignalR.Redis.Sharding
{
    public class DefaultRedisServerNameGenerator : IRedisServerNameGenerator
    {
        public string GenerateServerName() => $"{Environment.MachineName}_{Guid.NewGuid():N}";
    }
}