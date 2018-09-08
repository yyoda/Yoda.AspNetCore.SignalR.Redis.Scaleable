using System;
using System.Security.Cryptography;
using System.Text;

namespace Yoda.AspNetCore.SignalR.Redis.Sharding
{
    public class DefaultRedisServerResolver : IRedisServerResolver
    {
        public IRedisServer Resolve(IRedisServer[] server, string key)
        {
            using (var hashAlgorithm = MD5.Create())
            {
                var keyBytes = Encoding.UTF8.GetBytes(key);
                var hashBytes = hashAlgorithm.ComputeHash(keyBytes, 0, keyBytes.Length);
                var hashInteger = BitConverter.ToInt32(hashBytes, 0);
                var serverCount = server.Length;

                return server[Math.Abs(hashInteger) % serverCount];
            }
        }
    }
}