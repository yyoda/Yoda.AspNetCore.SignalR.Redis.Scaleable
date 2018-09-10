using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.SignalR;
using Microsoft.AspNetCore.SignalR.Protocol;
using Microsoft.AspNetCore.SignalR.Redis;
using Microsoft.AspNetCore.SignalR.Redis.Internal;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Yoda.AspNetCore.SignalR.Redis.Sharding.Internal;

namespace Yoda.AspNetCore.SignalR.Redis.Sharding
{
    public class ShardingRedisHubLifetimeManager<THub> : HubLifetimeManager<THub>, IDisposable
        where THub : Hub
    {
        private readonly HubConnectionStore _connections = new HubConnectionStore();
        private readonly RedisSubscriptionManager _groups = new RedisSubscriptionManager();
        private readonly RedisSubscriptionManager _users = new RedisSubscriptionManager();
        private IRedisServer[] _shardingServers;
        private IRedisServer _defaultServer;

        private readonly ShardingRedisOptions _options;
        private readonly RedisChannels _channels;
        private readonly RedisProtocol _protocol;
        private readonly SemaphoreSlim _connectionLock = new SemaphoreSlim(1);

        private readonly AckHandler _ackHandler;
        private readonly ILogger _logger;
        private int _internalId;

        public ShardingRedisHubLifetimeManager(ILogger<RedisHubLifetimeManager<THub>> logger,
                                               IOptions<ShardingRedisOptions> options,
                                               IHubProtocolResolver hubProtocolResolver)
        {
            _logger = logger;
            _options = options.Value;
            _ackHandler = new AckHandler();
            _channels = new RedisChannels(typeof(THub).FullName);
            _protocol = new RedisProtocol(hubProtocolResolver.AllProtocols);

            _ = EnsureRedisServerConnection();
        }

        public override async Task OnConnectedAsync(HubConnectionContext connection)
        {
            await EnsureRedisServerConnection();
            var feature = new RedisFeature();
            connection.Features.Set<IRedisFeature>(feature);

            _connections.Add(connection);

            var connectionTask = SubscribeToConnection(connection);
            var userTask = SubscribeToUser(connection);

            await Task.WhenAll(connectionTask, userTask);
        }

        public override Task OnDisconnectedAsync(HubConnectionContext connection)
        {
            _connections.Remove(connection);

            var connectionChannel = _channels.Connection(connection.ConnectionId);
            RedisLog.Unsubscribe(_logger, connectionChannel);

            var tasks = _shardingServers
                .Select(server => server.Subscriber.UnsubscribeAsync(connectionChannel))
                .ToList();

            var feature = connection.Features.Get<IRedisFeature>();
            var groupNames = feature.Groups;

            if (groupNames != null)
            {
                // Copy the groups to an array here because they get removed from this collection
                // in RemoveFromGroupAsync
                foreach (var group in groupNames.ToArray())
                {
                    // Use RemoveGroupAsyncCore because the connection is local and we don't want to
                    // accidentally go to other servers with our remove request.
                    tasks.Add(RemoveGroupAsyncCore(connection, group));
                }
            }

            if (!string.IsNullOrEmpty(connection.UserIdentifier))
            {
                tasks.Add(RemoveUserAsync(connection));
            }

            return Task.WhenAll(tasks);
        }

        public override Task SendAllAsync(string methodName, object[] args, CancellationToken cancellationToken = default(CancellationToken))
        {
            var message = _protocol.WriteInvocation(methodName, args);
            return PublishAsync(_defaultServer, _channels.All, message);
        }

        public override Task SendAllExceptAsync(string methodName, object[] args, IReadOnlyList<string> excludedConnectionIds, CancellationToken cancellationToken = default(CancellationToken))
        {
            var message = _protocol.WriteInvocation(methodName, args, excludedConnectionIds);
            return PublishAsync(_defaultServer, _channels.All, message);
        }

        public override Task SendConnectionAsync(string connectionId, string methodName, object[] args, CancellationToken cancellationToken = default(CancellationToken))
        {
            if (connectionId == null) throw new ArgumentNullException(nameof(connectionId));

            // If the connection is local we can skip sending the message through the bus since we require sticky connections.
            // This also saves serializing and deserializing the message!
            var connection = _connections[connectionId];
            if (connection != null)
            {
                return connection.WriteAsync(new InvocationMessage(methodName, args)).AsTask();
            }

            var channel = _channels.Connection(connectionId);
            var message = _protocol.WriteInvocation(methodName, args);
            var server = _options.ServerResovler.Resolve(_shardingServers, channel);

            return PublishAsync(server, channel, message);
        }

        public override Task SendGroupAsync(string groupName, string methodName, object[] args, CancellationToken cancellationToken = default(CancellationToken))
        {
            if (groupName == null) throw new ArgumentNullException(nameof(groupName));

            var channel = _channels.Group(groupName);
            var message = _protocol.WriteInvocation(methodName, args);
            var server = _options.ServerResovler.Resolve(_shardingServers, channel);

            return PublishAsync(server, channel, message);
        }

        public override async Task SendGroupExceptAsync(string groupName, string methodName, object[] args, IReadOnlyList<string> excludedConnectionIds, CancellationToken cancellationToken = default(CancellationToken))
        {
            if (groupName == null) throw new ArgumentNullException(nameof(groupName));

            var channel = _channels.Group(groupName);
            var message = _protocol.WriteInvocation(methodName, args, excludedConnectionIds);
            var server = _options.ServerResovler.Resolve(_shardingServers, channel);

            await PublishAsync(server, channel, message);
        }

        public override Task SendUserAsync(string userId, string methodName, object[] args, CancellationToken cancellationToken = default(CancellationToken))
        {
            var channel = _channels.User(userId);
            var message = _protocol.WriteInvocation(methodName, args);
            var server = _options.ServerResovler.Resolve(_shardingServers, channel);

            return PublishAsync(server, channel, message);
        }

        public override async Task AddToGroupAsync(string connectionId, string groupName, CancellationToken cancellationToken = default(CancellationToken))
        {
            if (connectionId == null) throw new ArgumentNullException(nameof(connectionId));
            if (groupName == null) throw new ArgumentNullException(nameof(groupName));

            var connection = _connections[connectionId];
            if (connection != null)
            {
                // short circuit if connection is on this server
                await AddGroupAsyncCore(connection, groupName);
                return;
            }

            await SendGroupActionAndWaitForAck(connectionId, groupName, GroupAction.Add);
        }

        public override async Task RemoveFromGroupAsync(string connectionId, string groupName, CancellationToken cancellationToken = default(CancellationToken))
        {
            if (connectionId == null) throw new ArgumentNullException(nameof(connectionId));
            if (groupName == null) throw new ArgumentNullException(nameof(groupName));

            var connection = _connections[connectionId];
            if (connection != null)
            {
                // short circuit if connection is on this server
                await RemoveGroupAsyncCore(connection, groupName);
                return;
            }

            await SendGroupActionAndWaitForAck(connectionId, groupName, GroupAction.Remove);
        }

        public override Task SendConnectionsAsync(IReadOnlyList<string> connectionIds, string methodName, object[] args, CancellationToken cancellationToken = default(CancellationToken))
        {
            if (connectionIds == null) throw new ArgumentNullException(nameof(connectionIds));

            var payload = _protocol.WriteInvocation(methodName, args);
            var tasks = connectionIds.Select(connectionId =>
            {
                var channel = _channels.Connection(connectionId);
                var server = _options.ServerResovler.Resolve(_shardingServers, channel);

                return PublishAsync(server, channel, payload);
            });

            return Task.WhenAll(tasks);
        }

        public override Task SendGroupsAsync(IReadOnlyList<string> groupNames, string methodName, object[] args, CancellationToken cancellationToken = default(CancellationToken))
        {
            if (groupNames == null) throw new ArgumentNullException(nameof(groupNames));

            var payload = _protocol.WriteInvocation(methodName, args);
            var tasks = groupNames.Where(groupName => !string.IsNullOrEmpty(groupName))
                .Select(groupName =>
                {
                    var channel = _channels.Group(groupName);
                    var server = _options.ServerResovler.Resolve(_shardingServers, channel);
                    return PublishAsync(server, channel, payload);
                });

            return Task.WhenAll(tasks);
        }

        public override Task SendUsersAsync(IReadOnlyList<string> userIds, string methodName, object[] args, CancellationToken cancellationToken = default(CancellationToken))
        {
            if (userIds.Count <= 0)
            {
                return Task.CompletedTask;
            }

            var payload = _protocol.WriteInvocation(methodName, args);
            var tasks = userIds.Where(userId => !string.IsNullOrEmpty(userId))
                .Select(userId =>
                {
                    var channel = _channels.User(userId);
                    var server = _options.ServerResovler.Resolve(_shardingServers, channel);
                    return PublishAsync(server, channel, payload);
                });

            return Task.WhenAll(tasks);
        }

        private async Task PublishAsync(IRedisServer server, string channel, byte[] payload)
        {
            await EnsureRedisServerConnection();
            RedisLog.PublishToChannel(_logger, channel);
            await server.Subscriber.PublishAsync(channel, payload);
        }

        private async Task AddGroupAsyncCore(HubConnectionContext connection, string groupName)
        {
            var feature = connection.Features.Get<IRedisFeature>();
            var groupNames = feature.Groups;

            lock (groupNames)
            {
                // Connection already in group
                if (!groupNames.Add(groupName))
                {
                    return;
                }
            }

            var channel = _channels.Group(groupName);
            await _groups.AddSubscriptionAsync(channel, connection, SubscribeToGroupAsync);
        }

        /// <summary>
        /// This takes <see cref="HubConnectionContext"/> because we want to remove the connection from the
        /// _connections list in OnDisconnectedAsync and still be able to remove groups with this method.
        /// </summary>
        private async Task RemoveGroupAsyncCore(HubConnectionContext connection, string groupName)
        {
            var groupChannel = _channels.Group(groupName);

            await _groups.RemoveSubscriptionAsync(groupChannel, connection, async channelName =>
            {
                RedisLog.Unsubscribe(_logger, channelName);
                var server = _options.ServerResovler.Resolve(_shardingServers, groupName);
                await server.Subscriber.UnsubscribeAsync(channelName);
            });

            var feature = connection.Features.Get<IRedisFeature>();
            var groupNames = feature.Groups;
            if (groupNames != null)
            {
                lock (groupNames)
                {
                    groupNames.Remove(groupName);
                }
            }
        }

        private async Task SendGroupActionAndWaitForAck(string connectionId, string groupName, GroupAction action)
        {
            var id = Interlocked.Increment(ref _internalId);
            var ack = _ackHandler.CreateAck(id);
            var server = _options.ServerResovler.Resolve(_shardingServers, groupName);
            var serverName = server.ServerName;
            // Send Add/Remove Group to other servers and wait for an ack or timeout
            var message = _protocol.WriteGroupCommand(new RedisGroupCommand(id, serverName, action, groupName, connectionId));
            await PublishAsync(server, _channels.GroupManagement, message);
            await ack;
        }

        private Task RemoveUserAsync(HubConnectionContext connection)
        {
            var userChannel = _channels.User(connection.UserIdentifier);

            return _users.RemoveSubscriptionAsync(userChannel, connection, async channelName =>
            {
                RedisLog.Unsubscribe(_logger, channelName);
                var server = _options.ServerResovler.Resolve(_shardingServers, channelName);
                await server.Subscriber.UnsubscribeAsync(channelName);
            });
        }

        public void Dispose()
        {
            foreach (var info in _shardingServers)
            {
                info?.Dispose();
            }

            _ackHandler.Dispose();
        }

        private void SubscribeToAll(IRedisServer server)
        {
            if (!server.IsDefault)
            {
                return;
            }

            RedisLog.Subscribing(_logger, _channels.All);
            server.Subscriber.Subscribe(_channels.All, async (_, data) =>
            {
                try
                {
                    RedisLog.ReceivedFromChannel(_logger, _channels.All);
                    var invocation = _protocol.ReadInvocation((byte[])data);
                    var tasks = _connections.AsEnumerable()
                        .Where(connectionContext =>
                        {
                            if (invocation.ExcludedConnectionIds == null)
                            {
                                return true;
                            }

                            return !invocation.ExcludedConnectionIds.Contains(connectionContext.ConnectionId);
                        })
                        .Select(connectionContext => connectionContext.WriteAsync(invocation.Message).AsTask());

                    await Task.WhenAll(tasks);
                }
                catch (Exception ex)
                {
                    RedisLog.FailedWritingMessage(_logger, ex);
                }
            });
        }

        private void SubscribeToGroupManagementChannel(IRedisServer server)
            => server.Subscriber.Subscribe(_channels.GroupManagement, async (c, data) =>
            {
                try
                {
                    var groupMessage = _protocol.ReadGroupCommand((byte[])data);

                    var connection = _connections[groupMessage.ConnectionId];
                    if (connection == null)
                    {
                        // user not on this server
                        return;
                    }

                    if (groupMessage.Action == GroupAction.Remove)
                    {
                        await RemoveGroupAsyncCore(connection, groupMessage.GroupName);
                    }

                    if (groupMessage.Action == GroupAction.Add)
                    {
                        await AddGroupAsyncCore(connection, groupMessage.GroupName);
                    }

                    // Send an ack to the server that sent the original command.
                    await PublishAsync(server, _channels.Ack(groupMessage.ServerName), _protocol.WriteAck(groupMessage.Id));
                }
                catch (Exception ex)
                {
                    RedisLog.InternalMessageFailed(_logger, ex);
                }
            });

        /// <summary>
        /// Create server specific channel in order to send an ack to a single server
        /// </summary>
        /// <param name="server"></param>
        private void SubscribeToAckChannel(IRedisServer server)
        {
            var ackChannel = _channels.Ack(server.ServerName);

            server.Subscriber.Subscribe(ackChannel, (_, data) =>
            {
                var ackId = _protocol.ReadAck((byte[]) data);
                _ackHandler.TriggerAck(ackId);
            });
        }

        private Task SubscribeToConnection(HubConnectionContext connection)
        {
            var channel = _channels.Connection(connection.ConnectionId);
            RedisLog.Subscribing(_logger, channel);
            var server = _options.ServerResovler.Resolve(_shardingServers, channel);

            return server.Subscriber.SubscribeAsync(channel, async (c, data) =>
            {
                var invocation = _protocol.ReadInvocation((byte[])data);
                await connection.WriteAsync(invocation.Message);
            });
        }

        private Task SubscribeToUser(HubConnectionContext connection)
        {
            if (string.IsNullOrEmpty(connection.UserIdentifier))
            {
                return Task.CompletedTask;
            }

            var userChannel = _channels.User(connection.UserIdentifier);

            return _users.AddSubscriptionAsync(userChannel, connection, async (channelName, subscriptions) =>
            {
                var server = _options.ServerResovler.Resolve(_shardingServers, channelName);
                await server.Subscriber.SubscribeAsync(channelName, async (_, data) =>
                {
                    try
                    {
                        var invocation = _protocol.ReadInvocation((byte[])data);
                        var tasks = subscriptions.AsEnumerable().Select(connectionContext =>
                        {
                            var task = connectionContext.WriteAsync(invocation.Message);
                            return task.AsTask();
                        });

                        await Task.WhenAll(tasks);
                    }
                    catch (Exception ex)
                    {
                        RedisLog.FailedWritingMessage(_logger, ex);
                    }
                });
            });
        }

        private Task SubscribeToGroupAsync(string groupChannel, HubConnectionStore groupConnections)
        {
            RedisLog.Subscribing(_logger, groupChannel);
            var server = _options.ServerResovler.Resolve(_shardingServers, groupChannel);

            return server.Subscriber.SubscribeAsync(groupChannel, async (_, data) =>
            {
                try
                {
                    var invocation = _protocol.ReadInvocation((byte[])data);
                    var tasks = groupConnections.AsEnumerable()
                        .Where(connection =>
                        {
                            if (invocation.ExcludedConnectionIds == null)
                            {
                                return true;
                            }

                            return !invocation.ExcludedConnectionIds.Contains(connection.ConnectionId);
                        })
                        .Select(connection =>
                        {
                            var task = connection.WriteAsync(invocation.Message);
                            return task.AsTask();
                        });

                    await Task.WhenAll(tasks);
                }
                catch (Exception ex)
                {
                    RedisLog.FailedWritingMessage(_logger, ex);
                }
            });
        }

        private async Task EnsureRedisServerConnection()
        {
            if (_defaultServer == null || _shardingServers == null)
            {
                await _connectionLock.WaitAsync();
                try
                {
                    if (_defaultServer == null || _shardingServers == null)
                    {
                        var writer = new LoggerTextWriter(_logger);

                        var tasks = _options.Configurations.Select(async configuration =>
                        {
                            var serverName = _options.ServerNameGenerator.GenerateServerName();
                            RedisLog.ConnectingToEndpoints(_logger, configuration.Options.EndPoints, serverName);
                            var redisConnection = await _options.ConnectAsync(configuration.Options, writer);
                            var server = new ShardingRedisServer(serverName, configuration.IsDefault, redisConnection, _logger);

                            SubscribeToAll(server);
                            SubscribeToGroupManagementChannel(server);
                            SubscribeToAckChannel(server);

                            return server;
                        }).ToArray();

                        var redisServers = await Task.WhenAll(tasks);

                        _defaultServer = redisServers.First(server => server.IsDefault);

                        var shardingServers = _options.DefaultServerSeparation
                            ? redisServers.Where(server => !server.IsDefault)
                            : redisServers;

                        _shardingServers = shardingServers.ToArray();
                    }
                }
                finally
                {
                    _connectionLock.Release();
                }
            }
        }

        private class LoggerTextWriter : TextWriter
        {
            private readonly ILogger _logger;

            public LoggerTextWriter(ILogger logger) => _logger = logger;

            public override Encoding Encoding => Encoding.UTF8;
            public override void Write(char value) { }
            public override void WriteLine(string value) => RedisLog.ConnectionMultiplexerMessage(_logger, value);
        }

        private interface IRedisFeature
        {
            HashSet<string> Groups { get; }
        }

        private class RedisFeature : IRedisFeature
        {
            public HashSet<string> Groups { get; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        }
    }
}