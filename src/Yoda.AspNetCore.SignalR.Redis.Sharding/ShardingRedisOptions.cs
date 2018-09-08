using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace Yoda.AspNetCore.SignalR.Redis.Sharding
{
    /// <summary>
    /// Options used to configure <see cref="ShardingRedisHubLifetimeManager{THub}"/>.
    /// </summary>
    public class ShardingRedisOptions
    {
        /// <summary>
        /// Gets or sets configuration options exposed by <c>StackExchange.Redis</c>.
        /// </summary>
        public List<ConfigurationOptions> Configurations { get; set; } = new List<ConfigurationOptions>();

        /// <summary>
        /// Gets or sets the Redis connection factory.
        /// </summary>
        public Func<TextWriter, Task<IConnectionMultiplexer>> ConnectionFactory { get; set; }

        /// <summary>
        /// Gets or sets the Redis connection resolver.
        /// </summary>
        public IRedisServerResolver Resovler { get; set; } = new DefaultRedisServerResolver();

        /// <summary>
        /// Gets or sets the Redis server name generator.
        /// </summary>
        public IRedisServerNameGenerator ServerNameGenerator { get; set; } = new DefaultRedisServerNameGenerator();

        public void AddConfiguration(string redisConnectionString) => Configurations.Add(ConfigurationOptions.Parse(redisConnectionString));

        public void AddConfiguration(ConfigurationOptions configuration) => Configurations.Add(configuration);

        public bool HasConfiguration() => Configurations.Any();

        internal async Task<IConnectionMultiplexer> ConnectAsync(ConfigurationOptions configuration, TextWriter log)
        {
            // Factory is publically settable. Assigning to a local variable before null check for thread safety.
            if (ConnectionFactory != null)
            {
                return await ConnectionFactory(log);
            }
            
            // REVIEW: Should we do this?
            if (configuration.EndPoints.Count == 0)
            {
                configuration.EndPoints.Add(IPAddress.Loopback, 0);
                configuration.SetDefaultPorts();
            }

            return await ConnectionMultiplexer.ConnectAsync(configuration, log);

        }
    }
}