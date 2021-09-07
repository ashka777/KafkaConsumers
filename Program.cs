using KafkaNet;
using KafkaNet.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KafkaConsumers
{
    class Program
    {
        static void Main(string[] args)
        {
            Setting setting = new Setting();
            ConsumerOptions consumerOpt = setting.ConnectionSetting(args);
            
            _ = new GetData(consumerOpt);

            Console.ReadKey();
        }
    }
}
