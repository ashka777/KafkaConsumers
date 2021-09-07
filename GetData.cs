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

namespace KafkaConsumers
{
    class GetData : Setting
    {
        string[] Settings { get; set; }
        private Consumer consumer;
        private ConsumerOptions _consumerOptions;
        private KafkaJobEntities db;// = new KafkaJobEntities();
        private GetXml getXml;

        public GetData(ConsumerOptions consumerOptions)
        {
       //     this.Settings = Args;

            _consumerOptions = consumerOptions;
            db = new KafkaJobEntities();
            DataToConsumer(_consumerOptions);
        }

        public void DataToConsumer(ConsumerOptions consumerOptions)
        {
          //  consumerOptions.PartitionWhitelist.AddRange(new List<int>() { 0, 1 , 2, 3, 4, 5, 6, 7, 8});
            try
            {
                getXml = new GetXml();

                if (getXml == null)
                {
                    Console.WriteLine("Таблица для записи данных не найдена в БД.");
                    return;
                }

                using (/*Consumer*/ consumer = new Consumer(consumerOptions))
                {
                    string value = "";
               //     var res = consumer.Consume().ToList();
                    foreach (var message in consumer.Consume(new CancellationToken(false)))
                    {
                        TransactionScope scope = new TransactionScope(TransactionScopeOption.RequiresNew
                               , new TransactionOptions { IsolationLevel = System.Transactions.IsolationLevel.RepeatableRead });
                        using (scope)
                        {
                            value = Encoding.UTF8.GetString(message.Value);
                            Console.WriteLine(value);
                            getXml.XmlText = value;
                            db.Entry(getXml).State = EntityState.Added;
                            db.SaveChanges();
                            scope.Complete();
                        }
                    }
                }
            }
            catch (DbEntityValidationException ex)
            {
                Console.WriteLine(ex.Message);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }
    }

}