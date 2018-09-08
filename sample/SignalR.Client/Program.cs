using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace SignalR.Client
{
    class Program
    {
        static void Main(string[] args)
        {
            var tokenSource = new CancellationTokenSource();
            var context = new ClientContext("http://localhost:5000/EchoHub");

            context.WriteToInfoLog($"Starting process. process info:'{context}'");

            var process = new ClientProcess(context, tokenSource.Token);

            process.StartMonitoringProcess(context);

            var tasks = Enumerable.Range(1, context.MaxGroups).SelectMany(_ =>
            {
                var group = Guid.NewGuid().ToString("N");

                return Enumerable.Range(1, context.MaxClients)
                    .Select(client => process.StartWebsocketProcess(group, $"{group}-{client}"));
            }).ToArray();

            Console.CancelKeyPress += (sender, eventArgs) =>
            {
                context.WriteToInfoLog("Shutting down...");

                tokenSource.Cancel();

                while (true)
                {
                    if (tasks.Any(task => task.Status == TaskStatus.WaitingForActivation))
                    {
                        eventArgs.Cancel = true;
                        Thread.Sleep(100);
                        continue;
                    }

                    return;
                }
            };

            Task.WaitAll(tasks);
        }
    }
}
