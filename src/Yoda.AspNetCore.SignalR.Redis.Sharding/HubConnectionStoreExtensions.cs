using System.Collections.Generic;
using Microsoft.AspNetCore.SignalR;

namespace Yoda.AspNetCore.SignalR.Redis.Sharding
{
    internal static class HubConnectionStoreExtensions
    {
        public static IEnumerable<HubConnectionContext> AsEnumerable(this HubConnectionStore connections)
        {
            foreach (var connection in connections)
            {
                yield return connection;
            }
        }
    }
}