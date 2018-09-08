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
        private IRedisServer[] _servers = new IRedisServer[0];

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

            var userTask = Task.CompletedTask;

            _connections.Add(connection);

            var connectionTask = SubscribeToConnection(connection);

            if (!string.IsNullOrEmpty(connection.UserIdentifier))
            {
                userTask = SubscribeToUser(connection);
            }

            await Task.WhenAll(connectionTask, userTask);
        }

        public override Task OnDisconnectedAsync(HubConnectionContext connection)
        {
            _connections.Remove(connection);

            var connectionChannel = _channels.Connection(connection.ConnectionId);
            RedisLog.Unsubscribe(_logger, connectionChannel);

            var tasks = _servers
                .Select(serverConnection => serverConnection.Subscriber.UnsubscribeAsync(connectionChannel))
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
            return PublishAsync(_channels.All, message);
        }

        public override Task SendAllExceptAsync(string methodName, object[] args, IReadOnlyList<string> excludedConnectionIds, CancellationToken cancellationToken = default(CancellationToken))
        {
            var message = _protocol.WriteInvocation(methodName, args, excludedConnectionIds);
            return PublishAsync(_channels.All, message);
        }

        public override Task SendConnectionAsync(string connectionId, string methodName, object[] args, CancellationToken cancellationToken = default(CancellationToken))
        {
            if (connectionId == null)
            {
                throw new ArgumentNullException(nameof(connectionId));
            }

            // If the connection is local we can skip sending the message through the bus since we require sticky connections.
            // This also saves serializing and deserializing the message!
            var connection = _connections[connectionId];
            if (connection != null)
            {
                return connection.WriteAsync(new InvocationMessage(methodName, args)).AsTask();
            }

            var message = _protocol.WriteInvocation(methodName, args);
            return PublishAsync(_channels.Connection(connectionId), message);
        }

        public override Task SendGroupAsync(string groupName, string methodName, object[] args, CancellationToken cancellationToken = default(CancellationToken))
        {
            if (groupName == null)
            {
                throw new ArgumentNullException(nameof(groupName));
            }

            var message = _protocol.WriteInvocation(methodName, args);
            return PublishAsync(_channels.Group(groupName), message);
        }

        public override async Task SendGroupExceptAsync(string groupName, string methodName, object[] args, IReadOnlyList<string> excludedConnectionIds, CancellationToken cancellationToken = default(CancellationToken))
        {
            if (groupName == null)
            {
                throw new ArgumentNullException(nameof(groupName));
            }

            var message = _protocol.WriteInvocation(methodName, args, excludedConnectionIds);
            await PublishAsync(_channels.Group(groupName), message);
        }

        public override Task SendUserAsync(string userId, string methodName, object[] args, CancellationToken cancellationToken = default(CancellationToken))
        {
            var message = _protocol.WriteInvocation(methodName, args);
            return PublishAsync(_channels.User(userId), message);
        }

        public override async Task AddToGroupAsync(string connectionId, string groupName, CancellationToken cancellationToken = default(CancellationToken))
        {
            if (connectionId == null)
            {
                throw new ArgumentNullException(nameof(connectionId));
            }

            if (groupName == null)
            {
                throw new ArgumentNullException(nameof(groupName));
            }

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
            if (connectionId == null)
            {
                throw new ArgumentNullException(nameof(connectionId));
            }

            if (groupName == null)
            {
                throw new ArgumentNullException(nameof(groupName));
            }

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
            if (connectionIds == null)
            {
                throw new ArgumentNullException(nameof(connectionIds));
            }

            var publishTasks = new List<Task>(connectionIds.Count);
            var payload = _protocol.WriteInvocation(methodName, args);

            foreach (var connectionId in connectionIds)
            {
                publishTasks.Add(PublishAsync(_channels.Connection(connectionId), payload));
            }

            return Task.WhenAll(publishTasks);
        }

        public override Task SendGroupsAsync(IReadOnlyList<string> groupNames, string methodName, object[] args, CancellationToken cancellationToken = default(CancellationToken))
        {
            if (groupNames == null)
            {
                throw new ArgumentNullException(nameof(groupNames));
            }
            var publishTasks = new List<Task>(groupNames.Count);
            var payload = _protocol.WriteInvocation(methodName, args);

            foreach (var groupName in groupNames)
            {
                if (!string.IsNullOrEmpty(groupName))
                {
                    publishTasks.Add(PublishAsync(_channels.Group(groupName), payload));
                }
            }

            return Task.WhenAll(publishTasks);
        }

        public override Task SendUsersAsync(IReadOnlyList<string> userIds, string methodName, object[] args, CancellationToken cancellationToken = default(CancellationToken))
        {
            if (userIds.Count > 0)
            {
                var payload = _protocol.WriteInvocation(methodName, args);
                var publishTasks = new List<Task>(userIds.Count);
                foreach (var userId in userIds)
                {
                    if (!string.IsNullOrEmpty(userId))
                    {
                        publishTasks.Add(PublishAsync(_channels.User(userId), payload));
                    }
                }

                return Task.WhenAll(publishTasks);
            }

            return Task.CompletedTask;
        }

        private async Task PublishAsync(string channel, byte[] payload)
        {
            await EnsureRedisServerConnection();
            RedisLog.PublishToChannel(_logger, channel);
            var serverConnection = _options.Resovler.Resolve(_servers, channel);
            await serverConnection.Subscriber.PublishAsync(channel, payload);
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

            var groupChannel = _channels.Group(groupName);
            await _groups.AddSubscriptionAsync(groupChannel, connection, SubscribeToGroupAsync);
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

                var serverConnection = _options.Resovler.Resolve(_servers, groupName);

                await serverConnection.Subscriber.UnsubscribeAsync(channelName);
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

            var serverConnection = _options.Resovler.Resolve(_servers, groupName);
            var serverName = serverConnection.ServerName;

            // Send Add/Remove Group to other servers and wait for an ack or timeout
            var message = _protocol.WriteGroupCommand(new RedisGroupCommand(id, serverName, action, groupName, connectionId));
            await PublishAsync(_channels.GroupManagement, message);

            await ack;
        }

        private Task RemoveUserAsync(HubConnectionContext connection)
        {
            var userChannel = _channels.User(connection.UserIdentifier);

            return _users.RemoveSubscriptionAsync(userChannel, connection, async channelName =>
            {
                RedisLog.Unsubscribe(_logger, channelName);
                var serverConnection = _options.Resovler.Resolve(_servers, channelName);
                await serverConnection.Subscriber.UnsubscribeAsync(channelName);
            });
        }

        public void Dispose()
        {
            foreach (var info in _servers)
            {
                info?.Dispose();
            }
            _ackHandler.Dispose();
        }

        private void SubscribeToAll(IRedisServer server)
        {
            RedisLog.Subscribing(_logger, _channels.All);

            server.Subscriber.Subscribe(_channels.All, async (c, data) =>
            {
                try
                {
                    RedisLog.ReceivedFromChannel(_logger, _channels.All);

                    var invocation = _protocol.ReadInvocation((byte[])data);

                    var tasks = new List<Task>(_connections.Count);

                    foreach (var connection in _connections)
                    {
                        if (invocation.ExcludedConnectionIds == null || !invocation.ExcludedConnectionIds.Contains(connection.ConnectionId))
                        {
                            tasks.Add(connection.WriteAsync(invocation.Message).AsTask());
                        }
                    }

                    await Task.WhenAll(tasks);
                }
                catch (Exception ex)
                {
                    RedisLog.FailedWritingMessage(_logger, ex);
                }
            });
        }

        private void SubscribeToGroupManagementChannel(IRedisServer server)
        {
            server.Subscriber.Subscribe(_channels.GroupManagement, async (c, data) =>
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
                    await PublishAsync(_channels.Ack(groupMessage.ServerName), _protocol.WriteAck(groupMessage.Id));
                }
                catch (Exception ex)
                {
                    RedisLog.InternalMessageFailed(_logger, ex);
                }
            });
        }

        private void SubscribeToAckChannel(IRedisServer server)
        {
            // Create server specific channel in order to send an ack to a single server
            server.Subscriber.Subscribe(_channels.Ack(server.ServerName), (c, data) =>
            {
                var ackId = _protocol.ReadAck((byte[])data);

                _ackHandler.TriggerAck(ackId);
            });
        }

        private Task SubscribeToConnection(HubConnectionContext connection)
        {
            var connectionChannel = _channels.Connection(connection.ConnectionId);

            RedisLog.Subscribing(_logger, connectionChannel);
            var serverConnection = _options.Resovler.Resolve(_servers, connectionChannel);
            return serverConnection.Subscriber.SubscribeAsync(connectionChannel, async (c, data) =>
            {
                var invocation = _protocol.ReadInvocation((byte[])data);
                await connection.WriteAsync(invocation.Message);
            });
        }

        private Task SubscribeToUser(HubConnectionContext connection)
        {
            var userChannel = _channels.User(connection.UserIdentifier);

            return _users.AddSubscriptionAsync(userChannel, connection, async (channelName, subscriptions) =>
            {
                var serverConnection = _options.Resovler.Resolve(_servers, channelName);
                await serverConnection.Subscriber.SubscribeAsync(channelName, async (c, data) =>
                {
                    try
                    {
                        var invocation = _protocol.ReadInvocation((byte[])data);

                        var tasks = new List<Task>();
                        foreach (var userConnection in subscriptions)
                        {
                            tasks.Add(userConnection.WriteAsync(invocation.Message).AsTask());
                        }

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
            var serverConnection = _options.Resovler.Resolve(_servers, groupChannel);
            return serverConnection.Subscriber.SubscribeAsync(groupChannel, async (c, data) =>
            {
                try
                {
                    var invocation = _protocol.ReadInvocation((byte[])data);

                    var tasks = new List<Task>();
                    foreach (var groupConnection in groupConnections)
                    {
                        if (invocation.ExcludedConnectionIds?.Contains(groupConnection.ConnectionId) == true)
                        {
                            continue;
                        }

                        tasks.Add(groupConnection.WriteAsync(invocation.Message).AsTask());
                    }

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
            if (_options.HasConfiguration())
            {
                await _connectionLock.WaitAsync();
                try
                {
                    if (_options.HasConfiguration())
                    {
                        var writer = new LoggerTextWriter(_logger);

                        var tasks = _options.Configurations.Select(async configuration =>
                        {
                            var serverName = _options.ServerNameGenerator.GenerateServerName();

                            RedisLog.ConnectingToEndpoints(_logger, configuration.EndPoints, serverName);

                            var connectionMultiplexer = await _options.ConnectAsync(configuration, writer);
                            var serverConnection = new ShardingRedisServer(serverName, connectionMultiplexer, _logger);

                            SubscribeToAll(serverConnection);
                            SubscribeToGroupManagementChannel(serverConnection);
                            SubscribeToAckChannel(serverConnection);

                            return serverConnection;
                        }).ToArray();

                        _servers = await Task.WhenAll(tasks);
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

            public LoggerTextWriter(ILogger logger)
            {
                _logger = logger;
            }

            public override Encoding Encoding => Encoding.UTF8;

            public override void Write(char value)
            {

            }

            public override void WriteLine(string value)
            {
                RedisLog.ConnectionMultiplexerMessage(_logger, value);
            }
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