using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KafkaConsumers.Model
{
    class ViewConfig
    {
        public string Adress { get; set; }
        public string Port { get; set; }
        public string Topic { get; set; }
     /*   public string Partition { get; set; }
        public string Broker { get; set; }
        public string Consumer { get; set; }*/
    }
}
