using Microsoft.Extensions.Configuration;
using System;
using System.Diagnostics;
using System.IO;
using System.Linq;

namespace SignalR.Server
{
    internal class RedisServer : IDisposable
    {
        private IDisposable[] _processes = new IDisposable[0];

        private const int DefaultRedisNum = 2;
        private const int DefaultPort = 7001;

        private const string RedisNumEnvKey = "REDIS_NUM";
        private const string RedisConnectionStringEnvKey = "REDIS_CONNECTION_STRING";
        private const string ProcessName = "redis-server";

        private RedisServer() { }

        public static RedisServer Instance { get; private set; } = new RedisServer();
        public string[] ConnectionStrings { get; set; } = new string[0];

        public static IDisposable Configure(IConfigurationRoot config)
        {
            Instance = new RedisServer();

            var serverCount = config.GetValue(RedisNumEnvKey, DefaultRedisNum);

            var connectionStrings = Enumerable.Range(1, serverCount)
                .Select(index => config.GetValue($"{RedisConnectionStringEnvKey}{index}", string.Empty))
                .Where(connectionString => !string.IsNullOrWhiteSpace(connectionString))
                .ToArray();

            if (connectionStrings.Any())
            {
                Instance.ConnectionStrings = connectionStrings;
            }
            else
            {
                Process.GetProcessesByName(ProcessName).ToList().ForEach(redisServer => redisServer.Kill());

                var sequence = Enumerable.Range(DefaultPort, serverCount);

                Instance.ConnectionStrings = sequence.Select(port => $"localhost:{port}").ToArray();
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
