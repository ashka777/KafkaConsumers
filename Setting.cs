using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using KafkaNet;
using KafkaNet.Model;
using System.Threading.Tasks;
using KafkaConsumers.Model;

namespace KafkaConsumers
{
    class Setting
    {
        internal string[] Args { get; set; }
        KafkaJobEntities db = new KafkaJobEntities();
        //Uri[] uri;//TODO: проба

        public ConsumerOptions ConnectionSetting(string[] args)
        {
            if (args.Length < 3)
            {
                Console.WriteLine($"Для настроек с БД - нажмите 1, для тестовых - любую клавишу: ");
                if (Console.ReadLine() == "1")
                {
                    foreach (var itemParam in ConnectParametersFromDB())
                    {
                        Args = new string[] { itemParam.Adress, itemParam.Port, itemParam.Topic };
                        //TODO: проба uri
                        //uri = new Uri[] {new Uri ($"http://{itemParam.Adress}:{itemParam.Port}")};
                        //Args[]
                        break; //TODO: Переделать функцию с разными настройками из БД
                    }
                }
                else
                {
                    Args = new string[] { "10.0.9.245", "9092", "locTest" }; //CASHIERS_LIST
                    Console.WriteLine($"Не были явно указаны параметры: 'сервер_порт_topic',\n будут использоваться: '{Args[0]} {Args[1]} {Args[2]}'.");
                    Console.WriteLine($"Для продолжения нажмите любую клавишу");
                    Console.ReadKey();
                }
            }
            else
                //uri = new Uri[] { new Uri($"http://{args[0]}:{args[1]}") };
                Args = args;
            Console.WriteLine($"Параметры: '{Args[0]} {Args[1]} {Args[2]}'.");
            return ConnectToBroker();
        }

        private ViewConfig[] ConnectParametersFromDB()
        {
            try
            {
                var listConnections = db.Config
                    .Where(w => w.Adress != null && w.Port != null && w.Topic != null)
                    .Select(s => new ViewConfig
                    {
                        Adress = s.Adress,
                        Port = s.Port,
                        Topic = s.Topic
                    }).ToArray();
                return listConnections;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
            }
        

        private ConsumerOptions ConnectToBroker()
        {
            Uri uri = new Uri($"http://{Args[0]}:{Args[1]}");
            KafkaOptions options = new KafkaOptions(uri);
            BrokerRouter router = new BrokerRouter(options);
            //router = router.SelectBrokerRoute(Args[2], 3);
            ConsumerOptions consumerOptions = new ConsumerOptions(Args[2], router);
            return consumerOptions;
        }
    }
}
