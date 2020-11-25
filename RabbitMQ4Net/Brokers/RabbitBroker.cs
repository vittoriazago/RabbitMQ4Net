using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace RabbitMQ4Net
{
    public class RabbitBroker : IDisposable
    {
        private IConnection _privateConnection;
        private IConnection Connection
        {
            get
            {
                lock (_helpObjectLockCon)
                {
                    if (_privateConnection == null)
                        _privateConnection = CreateConnection();
                }
                return _privateConnection;
            }
            set { _privateConnection = value; }
        }
        private readonly object _helpObjectLockCon = new object();

        private readonly Action<string, Exception> _logger;

        private readonly string _host;
        private readonly string _username;
        private readonly string _password;
        private readonly int _port;

        private IConnection _connection;

        public RabbitBroker(string host,
            string username,
            string password,
            int port,
            bool connectOnlyWhenPushing = true,
            Action<string, Exception> logger = null)
        {
            _host = host;
            _username = username;
            _password = password;
            _port = port;

            _logger = logger ?? ((string mensagem, Exception ex) => { });

            if (!connectOnlyWhenPushing)
                _privateConnection = CreateConnection();
        }

        private IConnection CreateConnection()
        {
            _logger($"Create Connection", null);
            var connection = new ConnectionFactory
            {
                HostName = _host,
                UserName = _username,
                Password = _password,
                Port = _port,
                AutomaticRecoveryEnabled = true,
                NetworkRecoveryInterval = TimeSpan.FromSeconds(30),
            }.CreateConnection();

            return connection;
        }

        public void CriarFila(string nomeFila)
        {
            using (var channel = Connection.CreateModel())
            {
                CriarFila(channel, nomeFila);
            }
        }

        public void CriarFila(IModel channel, string nomeFila)
        {
            var args = new Dictionary<string, object>
            {
                { "x-queue-mode", "lazy" }
            };
            channel.QueueDeclare(nomeFila, true, false, false, args);
        }

        public bool PublicarFila<T>(T evento, string nomeFila)
        {
            using (var channel = _connection.CreateModel())
            {
                var properties = channel.CreateBasicProperties();
                properties.Persistent = true;

                string message = JsonConvert.SerializeObject(evento);

                channel.BasicPublish(string.Empty, nomeFila, null, Encoding.UTF8.GetBytes(message));
            }
            return true;
        }
        static SemaphoreSlim semaphoreSlim = new SemaphoreSlim(1, 1);

        public void Subscribe(Func<string, Task<(ulong tag, bool success)>> taskEvent)
        {
            using var channel = Connection.CreateModel();
            channel.BasicQos(0, 10, false);

            var rawMessages = new List<BasicDeliverEventArgs>(100);
            var consumer = new EventingBasicConsumer(channel);

            int countBatch = 20;
            consumer.Received += async (ch, ea) =>
            {
                try
                {
                    rawMessages.Add(ea);

                    if (rawMessages.Count() == countBatch)
                    {
                        await semaphoreSlim.WaitAsync();

                        var tasks = rawMessages.Select(item =>
                        {
                            try
                            {
                                var message = Encoding.UTF8.GetString(item.Body);
                                return taskEvent(message);
                            }
                            catch (Exception)
                            {
                                channel.BasicNack(item.DeliveryTag, false, true);
                                throw;
                            }
                        });

                        var results = await Task.WhenAll(tasks);

                        var failedResults = results.Where(r => !r.success);
                        for (int i = (failedResults.Count() - 1); i >= 0; i--)
                        {
                            var failedResult = failedResults.ElementAt(i);
                            channel.BasicNack(failedResult.tag, false, true);
                        }

                        channel.BasicAck(rawMessages.FirstOrDefault().DeliveryTag, true);

                        rawMessages.Clear();

                        semaphoreSlim.Release();
                    }
                }
                catch (Exception)
                {
                    for (int i = (rawMessages.Count - 1); i >= 0; i--)
                    {
                        var rawMessage = rawMessages.ElementAt(i);
                        channel.BasicNack(rawMessage.DeliveryTag, false, true);

                        rawMessages.Remove(rawMessage);
                    }
                    throw;
                }

                channel.BasicConsume("", false, consumer);
            };
        }

        public void Dispose()
        {
            if (_connection != null)
            {
                _connection.Dispose();
                _connection = null;
            }
        }
    }
}