using System;
using System.Collections.Generic;
using System.Text;

namespace RabbitMQ4Net.Models
{
    public class RabbitMqSettings
    {
        public string Host { get; set; }
        public string Username { get; set; }
        public string Password { get; set; }
        public int Port { get; set; }

        public string ClientProviderName { get; set; }
        public int RequestHeartBeat { get; set; }

        public ushort? ConsumerPrefetchCount { get; set; }

    }
}
