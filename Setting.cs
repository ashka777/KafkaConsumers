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

        public ConsumerOptions ConnectionSetting(string[] args)
        {
            //if (args.Length < 3)
            //{
            //    Console.WriteLine($"Для настроек с БД - нажмите 1, для тестовых - любую клавишу: ");
            //    if (Console.ReadLine() == "1")
            //    {
            //        foreach (var itemParam in ConnectParametersFromDB())
            //        {
            //            Args = new string[] { itemParam.Adress, itemParam.Port, itemParam.Topic };
            //            break; //TODO: Переделать функцию с разными настройками из БД
            //        }
            //    }
                //if (Args == null)
                //{
                    Args = new string[] { "10.0.28.77", "9092", "CASHIERS_LIST" }; //10.0.9.245 2181 CASHIERS_LIST
            //Console.WriteLine($"Не были явно указаны или получены параметры: " +
            //                        $"'сервер_порт_topic',\n будут использоваться по умолчанию: " +
            //                        $"'{Args[0]} {Args[1]} {Args[2]}'.");
                    //Console.WriteLine($"Для продолжения нажмите любую клавишу");
                    //Console.ReadKey();
                //}
            //}
            //else
                //Args = args;
            Console.WriteLine($"Параметры: '{Args[0]} {Args[1]} {Args[2]}'.");
            return ConnectToBroker();
        }

        //private ViewConfig[] ConnectParametersFromDB()
        //{
        //    try
        //    {
        //        Console.WriteLine("Получаю данные для подключения из БД...");
        //        KafkaJobEntities db = new KafkaJobEntities();
        //        var listConnections = db.Config
        //            .Where(w => w.Adress != null && w.Port != null && w.Topic != null)
        //            .Select(s => new ViewConfig
        //            {
        //                Adress = s.Adress,
        //                Port = s.Port,
        //                Topic = s.Topic
        //            }).DefaultIfEmpty().ToArray();
        //        return listConnections;
        //    }
        //    catch (Exception ex)
        //    {
        //        Console.WriteLine($"ОШИБКА: {ex.Message}\n");
        //    }
        //    return Array.Empty<ViewConfig>();// new ViewConfig[] { }; 
        //}
        

        private ConsumerOptions ConnectToBroker()
        {
            Uri uri = new Uri($"http://{Args[0]}:{Args[1]}");
            KafkaOptions options = new KafkaOptions(uri);
            BrokerRouter router = new BrokerRouter(options);
            router.SelectBrokerRoute(Args[2], 0);
            ConsumerOptions consumerOptions = new ConsumerOptions(Args[2], router);
            //consumerOptions.PartitionWhitelist = new List<int> { 0 };

            //var offSetPartition = new KafkaNet.Protocol.OffsetPosition
            //{
            //    Offset = 31,
            //    PartitionId = 0
            //};

            //consumerOptions.Topic = "CASHIERS_LIST";
            //consumerOptions.BackoffInterval = new TimeSpan(34, 0, 0);

            return consumerOptions;
        }
    }
}
