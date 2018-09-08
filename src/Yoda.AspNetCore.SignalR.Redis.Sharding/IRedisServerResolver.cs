namespace Yoda.AspNetCore.SignalR.Redis.Sharding
{
    public interface IRedisServerResolver
    {
        IRedisServer Resolve(IRedisServer[] server, string key);
    }
}