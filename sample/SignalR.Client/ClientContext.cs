using System;
using Microsoft.Extensions.Logging;

namespace SignalR.Client
{
    public class ClientContext
    {
        public ClientContext(string url = null)
        {
            Url = url ?? Environment.GetEnvironmentVariable("URL") ?? throw new ArgumentNullException(nameof(Url));
            MaxGroups = int.Parse(Environment.GetEnvironmentVariable("MAX_GROUPS") ?? "1");
            MaxClients = int.Parse(Environment.GetEnvironmentVariable("MAX_CLIENTS") ?? "1");
            SendMessageIntervalMilliseconds = int.Parse(Environment.GetEnvironmentVariable("SEND_INTERVAL_MS") ?? "1000");
            MessageSize = int.Parse(Environment.GetEnvironmentVariable("MSG_SIZE") ?? "1");
            ReportIntarvalMilliseconds = 1000;
            LogLevel = Environment.GetEnvironmentVariable("LOG_LEVEL") ?? "INFO";
        }

        public string Url { get; }
        public int MaxGroups { get; }
        public int MaxClients { get; }
        public int SendMessageIntervalMilliseconds { get; }
        public int MessageSize { get; }
        public int ReportIntarvalMilliseconds { get; }
        public string LogLevel { get; }

        public void WriteToInfoLog(string message)
            => Console.WriteLine($"[INFO] {message}");

        public void WriteToDebugLog(string message)
        {
            if (LogLevel == "DEBUG")
            {
                Console.WriteLine($"[DEBUG] {message}");
            }
        }

        public override string ToString()
            => $"url:{Url}, " +
               $"maxGroups:{MaxGroups}, " +
               $"maxClients:{MaxClients}, " +
               $"sendMessageInterval(ms):{SendMessageIntervalMilliseconds}, " + 
               $"messageSize:{MessageSize}, " + 
               $"reportInterval(ms):{ReportIntarvalMilliseconds}, " + 
               $"logLevel:{LogLevel}";
    }
}