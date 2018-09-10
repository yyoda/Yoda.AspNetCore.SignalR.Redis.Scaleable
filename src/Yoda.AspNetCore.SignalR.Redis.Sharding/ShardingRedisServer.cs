using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using Yoda.AspNetCore.SignalR.Redis.Sharding.Internal;

namespace Yoda.AspNetCore.SignalR.Redis.Sharding
{
    public class ShardingRedisServer : IRedisServer
    {
        public ShardingRedisServer(string serverName, bool isDefault, IConnectionMultiplexer serverConnection, ILogger logger)
        {
            ServerName = serverName;

            IsDefault = isDefault;

            Connection = serverConnection;

            Connection.ConnectionRestored += (_, e) =>
            {
                // We use the subscription connection type
                // Ignore messages from the interactive connection (avoids duplicates)
                if (e.ConnectionType == ConnectionType.Interactive)
                {
                    return;
                }

                RedisLog.ConnectionRestored(logger);
            };

            Connection.ConnectionFailed += (_, e) =>
            {
                // We use the subscription connection type
                // Ignore messages from the interactive connection (avoids duplicates)
                if (e.ConnectionType == ConnectionType.Interactive)
                {
                    return;
                }

                RedisLog.ConnectionFailed(logger, e.Exception);
            };

            if (Connection.IsConnected)
            {
                RedisLog.Connected(logger);
            }
            else
            {
                RedisLog.NotConnected(logger);
            }

            Subscriber = Connection.GetSubscriber();
        }

        public string ServerName { get; }
        public bool IsDefault { get; }
        public IConnectionMultiplexer Connection { get; }
        public ISubscriber Subscriber { get; }

        public void Dispose() => Connection?.Dispose();
    }
}