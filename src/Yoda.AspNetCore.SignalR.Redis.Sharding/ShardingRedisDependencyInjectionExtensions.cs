using System;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.DependencyInjection;
using StackExchange.Redis;

namespace Yoda.AspNetCore.SignalR.Redis.Sharding
{
    public static class ShardingRedisDependencyInjectionExtensions
    {
        /// <summary>
        /// Adds scale-out to a <see cref="ISignalRServerBuilder"/>, using a shared Redis server.
        /// </summary>
        /// <param name="signalrBuilder">The <see cref="ISignalRServerBuilder"/>.</param>
        /// <param name="redisConnectionString">The connection string used to connect to the Redis server.</param>
        /// <returns>The same instance of the <see cref="ISignalRServerBuilder"/> for chaining.</returns>
        public static ISignalRServerBuilder UseScaleableRedis(this ISignalRServerBuilder signalrBuilder, string redisConnectionString)
            => UseShardingRedis(signalrBuilder, o =>
            {
                o.Configurations.Add(ConfigurationOptions.Parse(redisConnectionString));
            });

        /// <summary>
        /// Adds scale-out to a <see cref="ISignalRServerBuilder"/>, using a shared Redis server.
        /// </summary>
        /// <param name="signalrBuilder">The <see cref="ISignalRServerBuilder"/>.</param>
        /// <param name="configure">A callback to configure the Redis options.</param>
        /// <returns>The same instance of the <see cref="ISignalRServerBuilder"/> for chaining.</returns>
        public static ISignalRServerBuilder UseShardingRedis(this ISignalRServerBuilder signalrBuilder, Action<ShardingRedisOptions> configure)
        {
            signalrBuilder.Services.Configure(configure);
            signalrBuilder.Services.AddSingleton(typeof(HubLifetimeManager<>), typeof(ShardingRedisHubLifetimeManager<>));
            return signalrBuilder;
        }
    }
}