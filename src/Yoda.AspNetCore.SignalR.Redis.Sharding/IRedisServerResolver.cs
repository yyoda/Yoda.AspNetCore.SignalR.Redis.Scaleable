namespace Yoda.AspNetCore.SignalR.Redis.Sharding
{
    public interface IRedisServerResolver
    {
        IRedisServer Resolve(IRedisServer[] servers, string key);
        IRedisServer Default(IRedisServer[] servers);
    }
}