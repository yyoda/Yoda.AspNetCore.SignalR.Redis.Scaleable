using System;
using System.Diagnostics;
using System.IO;
using System.Linq;

namespace SignalR.Server
{
    internal class RedisServer : IDisposable
    {
        private IDisposable[] _processes;
        private static string[] _connectionStrings;

        private RedisServer(IDisposable[] processes) => _processes = processes;

        public static IDisposable Start(params int[] ports)
        {
            ConnectionStrings = ports.Select(port => $"localhost:{port}").ToArray();
            var packagePath = $@"{Environment.GetEnvironmentVariable("userprofile")}\.nuget\packages\redis-64";
            var exePath = Directory.GetFiles(packagePath, "redis-server.exe", SearchOption.AllDirectories)[0];
            var processes = ports.Select(port => Process.Start(exePath, $"--port {port}")).ToArray();

            return new RedisServer(processes);
        }

        public static string[] ConnectionStrings
        {
            get => _connectionStrings ?? throw new Exception("Redis server is not running.");
            set => _connectionStrings = value;
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
