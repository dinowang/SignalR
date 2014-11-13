using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNet.SignalR.Configuration;
using Microsoft.AspNet.SignalR.Hosting.Memory;
using Microsoft.AspNet.SignalR.Infrastructure;
using Microsoft.AspNet.SignalR.Messaging;
using Microsoft.AspNet.SignalR.Tests.Common.Infrastructure;
using Microsoft.AspNet.SignalR.Tracing;
using Owin;
using Xunit;
using IClientRequest = Microsoft.AspNet.SignalR.Client.Http.IRequest;
using IClientResponse = Microsoft.AspNet.SignalR.Client.Http.IResponse;

namespace Microsoft.AspNet.SignalR.FunctionalTests.Server.Connections
{
    public class FarmFacts : HostedTest
    {

        [Fact]
        public async Task FarmDisconnectRaisesUncleanDisconnects()
        {
            EnableTracing();

            // Each node shares the same bus but are indepenent servers
            var counters = new SignalR.Infrastructure.PerformanceCounterManager();
            var configurationManager = new DefaultConfigurationManager();
            var protectedData = new DefaultProtectedData();
            using (var bus = new MessageBus(new StringMinifier(), new TraceManager(), counters, configurationManager, 5000))
            {
                var nodeCount = 3;
                var nodes = new MemoryHost[nodeCount];
                var broadcasters = new IConnection[nodeCount];
                for (int i = 0; i < nodeCount; i++)
                {
                    nodes[i] = new MemoryHost();
                }

                var timeout = TimeSpan.FromSeconds(5);
                for (int i = 0; i < nodes.Length; i++)
                {
                    var node = nodes[i];
                    var resolver = new DefaultDependencyResolver();

                    resolver.Register(typeof(IMessageBus), () => bus);
                    resolver.Register(typeof(FarmConnection), () => new FarmConnection());

                    var connectionManager = resolver.Resolve<IConnectionManager>();
                    broadcasters[i] = connectionManager.GetConnectionContext<FarmConnection>().Connection;

                    var config = resolver.Resolve<IConfigurationManager>();
                    config.DisconnectTimeout = TimeSpan.FromSeconds(6);

                    node.Configure(app =>
                    {
                        app.MapSignalR<FarmConnection>("/echo", new ConnectionConfiguration
                        {
                            Resolver = resolver
                        });

                        resolver.Register(typeof(IProtectedData), () => protectedData);
                    });
                }

                var loadBalancer = new LoadBalancer(nodes);
                var transport = new Client.Transports.LongPollingTransport(loadBalancer);

                var connection = new Client.Connection("http://goo/echo");

                await connection.Start(transport);

                for (int i = 0; i < broadcasters.Length; i++)
                {
                    broadcasters[i].Broadcast(String.Format("From Node {0}: {1}", i, i + 1)).Wait();
                    await Task.Delay(TimeSpan.FromSeconds(1));
                }

                ((Client.IConnection)connection).Disconnect();

                await Task.Delay(TimeSpan.FromTicks(timeout.Ticks * nodes.Length));

                Assert.Equal(0, FarmConnection.CleanDisconnectCount);
                Assert.Equal(3, FarmConnection.UncleanDisconnectCount);
            }
        }

        private class ServerNode
        {
            public MemoryHost Server { get; private set; }
            public IDependencyResolver Resolver { get; private set; }

            private IConnection _connection;

            public ServerNode(IMessageBus bus)
            {
                // Give each server it's own dependency resolver
                Server = new MemoryHost();
                Resolver = new DefaultDependencyResolver();

                Resolver.Register(typeof(FarmConnection), () => new FarmConnection());
                Resolver.Register(typeof(IMessageBus), () => bus);

                var context = Resolver.Resolve<IConnectionManager>().GetConnectionContext<FarmConnection>();
                _connection = context.Connection;
            }

            public void Broadcast(string message)
            {
                _connection.Broadcast(message).Wait();
            }
        }

        private class LoadBalancer : SignalR.Client.Http.IHttpClient
        {
            private int _counter;
            private readonly SignalR.Client.Http.IHttpClient[] _servers;

            public void Initialize(SignalR.Client.IConnection connection)
            {
                foreach (SignalR.Client.Http.IHttpClient server in _servers)
                {
                    server.Initialize(connection);
                }
            }

            public LoadBalancer(params SignalR.Client.Http.IHttpClient[] servers)
            {
                _servers = servers;
            }

            public Task<IClientResponse> Get(string url, Action<IClientRequest> prepareRequest, bool isLongRunning)
            {
                Debug.WriteLine("Server {0}: GET {1}", _counter, url);
                int index = _counter;
                _counter = (_counter + 1) % _servers.Length;
                return _servers[index].Get(url, prepareRequest, isLongRunning);
            }

            public Task<IClientResponse> Post(string url, Action<IClientRequest> prepareRequest, IDictionary<string, string> postData, bool isLongRunning)
            {
                Debug.WriteLine("Server {0}: POST {1}", _counter, url);
                int index = _counter;
                _counter = (_counter + 1) % _servers.Length;
                return _servers[index].Post(url, prepareRequest, postData, isLongRunning); ;
            }
        }

        private class FarmConnection : PersistentConnection
        {
            public static int CleanDisconnectCount { get; set; }
            public static int UncleanDisconnectCount { get; set; }

            protected override Task OnDisconnected(IRequest request, string connectionId, bool stopCalled)
            {
                if (stopCalled)
                {
                    CleanDisconnectCount++;
                }
                else
                {
                    UncleanDisconnectCount++;
                }

                return base.OnDisconnected(request, connectionId, stopCalled);
            }

            protected override Task OnReceived(IRequest request, string connectionId, string data)
            {
                return Connection.Broadcast(data);
            }
        }

    }
}
