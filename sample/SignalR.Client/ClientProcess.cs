using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.SignalR.Client;

namespace SignalR.Client
{
    public class ClientProcess
    {
        private int _activeClientsTotal;
        private int _receivedMessagesTotal;
        private int _sentMessagesTotal;
        private int _exceptionsTotal;
        private long _lastReceiveElapsedMilliseconds;
        private long _lastSendElapsedMilliseconds;

        private readonly CancellationToken _cancellationToken;
        private readonly ClientContext _context;

        public ClientProcess(ClientContext context, CancellationToken cancellationToken)
        {
            _context = context;
            _cancellationToken = cancellationToken;
        }

        public async Task StartWebsocketProcess(string group, string client)
        {
            var swForReceive = new Stopwatch();
            var swForSend = new Stopwatch();

            var connection = new HubConnectionBuilder().WithUrl(_context.Url).Build();

            void OnReceive(string log)
            {
                _context.WriteToDebugLog(log);
                Measure(swForReceive, ms => _lastReceiveElapsedMilliseconds = Interlocked.Exchange(ref _lastReceiveElapsedMilliseconds, ms));
                _receivedMessagesTotal = Interlocked.Increment(ref _receivedMessagesTotal);
            }

            connection.On<string, string>("ReceiveFromAll", (from, msg) => OnReceive($"[RECV-ALL] from:{from}, to:{client}, msg:{msg}"));
            connection.On<string, string>("ReceiveFromMe", (from, msg) => OnReceive($"[RECV-ME] from:{from}, to:{client}, msg:{msg}"));
            connection.On<string, string>("ReceiveFromGroup", (from, msg) => OnReceive($"[RECV-GROUP] from:{from} to:{client}, msg:{msg}"));
            connection.On<string, string>("ReceiveFromGroupWithoutMe", (from, msg) => OnReceive($"[RECV-GROUP-WITHOUT-ME] from:{from} to:{client}, msg:{msg}"));

            var sendMessage = new string('x', _context.MessageSize);
            await connection.StartAsync(CancellationToken.None);
            await connection.InvokeAsync("Join", group, CancellationToken.None);
            _activeClientsTotal = Interlocked.Increment(ref _activeClientsTotal);

            _context.WriteToDebugLog($"Connected client:'{client}'.");

            while (true)
            {
                try
                {
                    if (_cancellationToken.IsCancellationRequested)
                    {
                        await connection.InvokeAsync("Leave", group, CancellationToken.None);
                        await connection.StopAsync(CancellationToken.None);
                        _activeClientsTotal = Interlocked.Decrement(ref _activeClientsTotal);

                        _context.WriteToDebugLog($"Disconnected client:'{client}'.");

                        return;
                    }

                    _context.WriteToDebugLog($"[SEND] group:{group}, client:{client}, msg:{sendMessage}");

                    await connection.InvokeAsync("SendToGroup", group, client, sendMessage, CancellationToken.None);
                    //await connection.InvokeAsync("SendToAll", client, sendMessage, CancellationToken.None);
                    _sentMessagesTotal = Interlocked.Increment(ref _sentMessagesTotal);

                    Measure(swForSend, ms
                        => _lastSendElapsedMilliseconds = Interlocked.Exchange(ref _lastSendElapsedMilliseconds, ms));

                    await Task.Delay(_context.SendMessageIntervalMilliseconds, CancellationToken.None);
                }
                catch (Exception ex)
                {
                    _exceptionsTotal = Interlocked.Increment(ref _exceptionsTotal);
                    _context.WriteToDebugLog(ex.ToString());
                }
            }
        }

        public void StartMonitoringProcess(ClientContext context)
        {
            Task.Run(async () =>
            {
                while (true)
                {
                    await Task.Delay(context.ReportIntarvalMilliseconds, CancellationToken.None);

                    context.WriteToInfoLog
                    (
                        $"clients:{_activeClientsTotal}/{context.MaxGroups * context.MaxClients}"
                        + $", recv/sec:{_receivedMessagesTotal}"
                        + $", recv/latency(ms):{_lastReceiveElapsedMilliseconds}"
                        + $", send/sec:{_sentMessagesTotal}"
                        + $", send/latency(ms):{_lastSendElapsedMilliseconds}"
                        + $", errors:{_exceptionsTotal}"
                    );

                    Interlocked.Exchange(ref _receivedMessagesTotal, 0);
                    Interlocked.Exchange(ref _sentMessagesTotal, 0);
                }
            }, CancellationToken.None);
        }

        private static void Measure(Stopwatch stopwatch, Action<long> callback)
        {
            stopwatch.Stop();

            var elapsed = stopwatch.ElapsedMilliseconds;

            if (elapsed > 0)
            {
                callback(elapsed);
            }

            stopwatch.Restart();
        }
    }
}