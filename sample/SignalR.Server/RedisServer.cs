using Microsoft.Extensions.Configuration;
using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using Yoda.AspNetCore.SignalR.Redis.Sharding;

namespace SignalR.Server
{
    internal class RedisServer : IDisposable
    {
        private Process[] _processes = new Process[0];

        private const int DefaultRedisNum = 3;
        private const int DefaultPort = 7001;

        private const string RedisNumEnvKey = "REDIS_NUM";
        private const string RedisConnectionStringEnvKey = "REDIS_CONNECTION_STRING";
        private const string ProcessName = "redis-server";

        private RedisServer() { }

        public static RedisServer Instance { get; private set; } = new RedisServer();

        public ShardingRedisOptions.WrappedConfigurationOptions[] Configurations { get; set; } = new ShardingRedisOptions.WrappedConfigurationOptions[0];

        public static RedisServer Configure(IConfigurationRoot config)
        {
            Instance = new RedisServer();

            var serverCount = config.GetValue(RedisNumEnvKey, DefaultRedisNum);

            var configurations = Enumerable.Range(1, serverCount)
                .Select(index => config.GetValue($"{RedisConnectionStringEnvKey}{index}", string.Empty))
                .Where(connectionString => !string.IsNullOrWhiteSpace(connectionString))
                .Select((connectionString, index) => ShardingRedisOptions.CreateConfiguration(connectionString, index == 0))
                .ToArray();

            if (configurations.Any())
            {
                Instance.Configurations = configurations;
            }
            else
            {
                Process.GetProcessesByName(ProcessName).ToList().ForEach(redisServer => redisServer.Kill());

                var sequence = Enumerable.Range(DefaultPort, serverCount).ToArray();

                Instance.Configurations = sequence.Select((port, index) =>
                {
                    return ShardingRedisOptions.CreateConfiguration($"localhost:{port}", index == 0);
                }).ToArray();

                Instance._processes = sequence.Select(port =>
                {
                    var packagePath = $@"{Environment.GetEnvironmentVariable("userprofile")}\.nuget\packages\redis-64";
                    var path = Directory.GetFiles(packagePath, $"{ProcessName}.exe", SearchOption.AllDirectories)[0];
                    return Process.Start(path, $"--port {port}");
                }).ToArray();
            }

            return Instance;
        }

        public void Dispose()
        {
            foreach (var process in _processes)
            {
                process.Dispose();
            }
        }
    }
}
