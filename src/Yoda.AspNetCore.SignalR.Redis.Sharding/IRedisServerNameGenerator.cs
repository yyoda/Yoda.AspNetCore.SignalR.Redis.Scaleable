namespace Yoda.AspNetCore.SignalR.Redis.Sharding
{
    public interface IRedisServerNameGenerator
    {
        string GenerateServerName();
    }
}