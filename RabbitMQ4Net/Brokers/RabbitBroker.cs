using Newtonsoft.Json;
using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Text;

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

        private readonly int _milisecondsToWaitReading = 0;
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
            int milisecondsToWaitReading = 100,
            Action<string, Exception> logger = null)
        {
            _host = host;
            _username = username;
            _password = password;
            _port = port;

            _logger = logger ?? ((string mensagem, Exception ex) => { });
            _milisecondsToWaitReading = milisecondsToWaitReading;

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