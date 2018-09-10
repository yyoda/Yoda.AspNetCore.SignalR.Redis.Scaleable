using System;
using System.Threading.Tasks;
using Microsoft.AspNetCore.SignalR;

namespace SignalR.Server
{
    public class EchoHub : Hub
    {
        private const string DefaultGroupoName = "SignalR Users";

        public override async Task OnConnectedAsync()
        {
            await Groups.AddToGroupAsync(Context.ConnectionId, DefaultGroupoName);
            await base.OnConnectedAsync();
        }

        public override async Task OnDisconnectedAsync(Exception exception)
        {
            await Groups.RemoveFromGroupAsync(Context.ConnectionId, DefaultGroupoName);
            await base.OnDisconnectedAsync(exception);
        }

        public Task Join(string groupName)
            => Groups.AddToGroupAsync(Context.ConnectionId, groupName);

        public Task Leave(string groupName)
            => Groups.RemoveFromGroupAsync(Context.ConnectionId, groupName);

        public Task SendToAll(string from, string message)
            => Clients.All.SendAsync("ReceiveFromAll", from, message);

        public Task SendToMe(string from, string message)
            => Clients.Caller.SendAsync("ReceiveFromMe", from, message);

        public Task SendToGroup(string group, string from, string message)
            => Clients.Group(group).SendAsync("ReceiveFromGroup", from, message);

        public Task SendToGroupWithoutMe(string group, string from, string message)
            => Clients.GroupExcept(group, Context.ConnectionId).SendAsync("ReceiveFromGroupWithoutMe", from, message);
    }
}