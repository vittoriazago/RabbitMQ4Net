using System;
using System.Collections.Generic;
using System.Text;

namespace RabbitMQ4Net.Models
{
    public class QueueMqSettings
    {
        public string QueueName { get; set; }
        public string ExchangeName { get; set; }
        public string RoutingKey { get; set; }

        public int? MessageTTL { get; set; }
    }
}
