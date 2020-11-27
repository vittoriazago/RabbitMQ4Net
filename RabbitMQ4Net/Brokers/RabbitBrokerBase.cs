using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ4Net.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace RabbitMQ4Net
{
    public abstract class RabbitBrokerBase
    {
        IModel _channel;
        IConnection _rabbitConnection;

        readonly bool _globalChannel;
        protected readonly RabbitMqSettings _rabbitMqSettings;

        readonly object _helpObjectLockCon = new object();

        protected readonly Action<string, Exception> _logger;

        public RabbitBrokerBase(RabbitMqSettings rabbitMqSettings,
            bool globalChannel = true,
            Action<string, Exception> logger = null)
        {
            _globalChannel = globalChannel;
            _rabbitMqSettings = rabbitMqSettings;

            _logger = logger ?? ((string mensagem, Exception ex) => { });
        }

        private IConnection CreateConnection()
        {
            _logger($"Create Connection", null);
            var connection = new ConnectionFactory
            {
                HostName = _rabbitMqSettings.Host,
                UserName = _rabbitMqSettings.Username,
                Password = _rabbitMqSettings.Password,
                Port = _rabbitMqSettings.Port,
                AutomaticRecoveryEnabled = true,
                NetworkRecoveryInterval = TimeSpan.FromSeconds(30),
            }.CreateConnection();

            return connection;
        }
        private IConnection GetConnection()
        {
            lock (_helpObjectLockCon)
            {
                if (_rabbitConnection == null)
                    _rabbitConnection = CreateConnection();
            }
            return _rabbitConnection;
        }

        protected IModel GetChannel()
        {
            GetConnection();

            if (_globalChannel)
            {
                if (_channel != null)
                    _channel = _rabbitConnection.CreateModel();

                return _channel;
            }

            return _rabbitConnection.CreateModel();
        }

        public void CreateQueue(string queueName,
            bool durable = true,
            bool lazy = false,
            int? messageTtl = null,
            Dictionary<string, object> args = null)
        {
            var channel = GetChannel();
            args ??= new Dictionary<string, object>();

            if (lazy) args.Add("x-queue-mode", "lazy");
            if (messageTtl.HasValue) args.Add("x-message-ttl", messageTtl.Value);

            channel.QueueDeclare(queueName, durable, false, false, args);
        }
    }
}