using System;
using System.Collections.Generic;
using System.IO;
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
        public class WrappedConfigurationOptions
        {
            internal WrappedConfigurationOptions(string redisConnectionString, bool isDedicatedForAllChannel)
                : this(ConfigurationOptions.Parse(redisConnectionString), isDedicatedForAllChannel)
            {
            }

            internal WrappedConfigurationOptions(ConfigurationOptions options, bool isDedicatedForAllChannel)
            {
                Options = options;
                IsDedicatedForAllChannel = isDedicatedForAllChannel;
            }

            public ConfigurationOptions Options { get; }
            public bool IsDedicatedForAllChannel { get; }
        }

        public static WrappedConfigurationOptions CreateConfiguration(string redisConnectionString, bool isDedicatedForAllChannel = false)
            => new WrappedConfigurationOptions(redisConnectionString, isDedicatedForAllChannel);

        public static WrappedConfigurationOptions CreateConfiguration(ConfigurationOptions options, bool isDedicatedForAllChannel = false)
            => new WrappedConfigurationOptions(options, isDedicatedForAllChannel);

        /// <summary>
        /// Gets or sets configuration options exposed by <c>StackExchange.Redis</c>.
        /// </summary>
        public List<WrappedConfigurationOptions> Configurations { get; set; } = new List<WrappedConfigurationOptions>();

        /// <summary>
        /// Gets or sets the Redis connection factory.
        /// </summary>
        public Func<TextWriter, Task<IConnectionMultiplexer>> ConnectionFactory { get; set; }

        /// <summary>
        /// Gets or sets the Redis connection resolver.
        /// </summary>
        public IRedisServerResolver ServerResovler { get; set; } = new DefaultRedisServerResolver();

        /// <summary>
        /// Gets or sets the Redis server name generator.
        /// </summary>
        public IRedisServerNameGenerator ServerNameGenerator { get; set; } = new DefaultRedisServerNameGenerator();

        public void Add(WrappedConfigurationOptions wrappedConfiguration)
            => Configurations.Add(wrappedConfiguration);

        public void Add(ConfigurationOptions configuration, bool isDedicatedForAllChannel = false)
            => Configurations.Add(new WrappedConfigurationOptions(configuration, isDedicatedForAllChannel));

        public void Add(string redisConnectionString, bool isDedicatedForAllChannel = false)
            => Add(ConfigurationOptions.Parse(redisConnectionString), isDedicatedForAllChannel);

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