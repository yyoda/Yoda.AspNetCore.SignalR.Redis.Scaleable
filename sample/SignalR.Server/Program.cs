using Microsoft.AspNetCore;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;

namespace SignalR.Server
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var configuration = new ConfigurationBuilder()
                .AddEnvironmentVariables()
                .Build();

            using (RedisServer.Configure(configuration))
            {
                WebHost.CreateDefaultBuilder(args)
                    .UseConfiguration(configuration)
                    .UseStartup<Startup>()
                    .Build()
                    .Run();
            }
        }
    }
}
