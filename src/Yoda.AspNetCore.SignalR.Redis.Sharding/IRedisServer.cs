using System;
using StackExchange.Redis;

namespace Yoda.AspNetCore.SignalR.Redis.Sharding
{
    public interface IRedisServer : IDisposable
    {
        string ServerName { get; }
        bool IsDefault { get; }
        ISubscriber Subscriber { get; }
    }
}