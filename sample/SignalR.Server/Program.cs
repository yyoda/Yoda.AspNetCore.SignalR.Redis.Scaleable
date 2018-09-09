using Microsoft.AspNetCore;
using Microsoft.AspNetCore.Hosting;

namespace SignalR.Server
{
    public class Program
    {
        public static void Main(string[] args)
        {
            using (RedisServer.Start(7000, 7001))
            {
                WebHost.CreateDefaultBuilder(args)
                    .UseStartup<Startup>()
                    .Build()
                    .Run();
            }
        }
    }
}
