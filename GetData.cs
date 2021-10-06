using KafkaNet;
using KafkaNet.Model;
using System;
using System.Text;
using KafkaConsumers.Model;
using System.Linq;
using System.Collections.Generic;
using System.Transactions;
using System.Data.Entity.Validation;
using System.Data.Entity;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaConsumers
{
    class GetData : Setting
    {
        private ConsumerOptions _consumerOptions;
        //private KafkaJobEntities db;
        //private GetXml getXml;
        //private string resultEnter;

        public GetData(ConsumerOptions consumerOptions)
        {
            _consumerOptions = consumerOptions;
            //using (db = new KafkaJobEntities())
            //{
                //GetTableInfo();
                //if (resultEnter == "Y")
                //{
                    DataToConsumer(_consumerOptions);
                    Console.ReadKey();
                //    }
                //    else
                //        Console.WriteLine("Работа завершена.");
            //};
        }

        //private /*async*/ void GetTableInfo()
        //{
        //    try
        //    {
        //        Console.WriteLine("\nПроверяю таблицу для записи данных в БД...");
        //        this.getXml = /*await*/ db.GetXml.Select(s => s).First();
        //    }
        //    catch (Exception ex)
        //    {
        //        Console.Clear();
        //        Console.WriteLine($"ОШИБКА: { ex.Message}");
        //        return;
        //    }
        //    finally
        //    {
        //        if (getXml == null)
        //        {
        //            Console.WriteLine($"Таблица для записи данных не найдена в БД.\n" +
        //                $"Все равно получить данные? Y/N : ");

        //            resultEnter = Console.ReadLine().ToUpper();
        //        }
        //    }
        //}
        
        public void DataToConsumer(ConsumerOptions consumerOptions)
        {
           //// consumerOptions.PartitionWhitelist = new List<int> { 1, 2, 3, 4 };
            var offSetPartition = new KafkaNet.Protocol.OffsetPosition
            {
                Offset = 243,
                PartitionId = 3
            };
            ////consumerOptions.Topic = "CASHIERS_LIST";
            ////consumerOptions.BackoffInterval = new TimeSpan(134, 0, 0);

            Console.WriteLine("Получаю данные от брокера...");
            try
            {
                using (Consumer consumer = new Consumer(consumerOptions, offSetPartition))// new KafkaNet.Protocol.OffsetPosition { Offset = 224, PartitionId = 3 }))
                {
                    var offSet = consumer.GetOffsetPosition();
                    string value = "";
                    TransactionScope scope = new TransactionScope(TransactionScopeOption.RequiresNew
                               , new TransactionOptions { IsolationLevel = System.Transactions.IsolationLevel.RepeatableRead });
                    using (scope)
                    {
                        foreach (var message in consumer.Consume()) //TODO: Получение данных из Кафка, Проваливается 
                        {
                            value = Encoding.UTF8.GetString(message.Value);
                            Console.WriteLine(value);
                            //if (getXml == null)
                            //    break;

                            //getXml.XmlText = value;
                            //db.Entry(getXml).State = EntityState.Added;
                            //db.SaveChanges();
                            //scope.Complete();
                            //break;
                        }
                    }
                }
            }
            //catch (DbEntityValidationException ex)
            //{
            //    Console.WriteLine(ex.Message);

            //}
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }
    }

}