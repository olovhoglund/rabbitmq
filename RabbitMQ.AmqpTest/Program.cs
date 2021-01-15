using Amqp;
using Amqp.Sasl;
using Amqp.Framing;
using System;
using System.Text;
using System.Threading;

namespace RabbitMQ.AmqpTest
{
    class Program
    {
        static void Main(string[] args)
        {
            // send a message to queue foo1 with application property "messagetype" set to "foo"
            SendMessage("foo1", "Hello World !");

            // receive the above message from queue foo1
            var msg = ReceiveMessage("foo1");

            // check the application property
            var messageType = msg.ApplicationProperties["messagetype"];
            Console.WriteLine("MessageType from queue foo1 is : " + messageType);

            // ok, now send the same message to queue foo2
            // this queue has a shovel that sends this message forward to queue foo3
            SendMessage("foo2", "Hello World !");

            // now the message should be on queue foo3
            var msg2 = ReceiveMessage("foo3");

            // check the application property
            var messageType2 = msg2.ApplicationProperties["messagetype"];

            // the same application property is now null !
            Console.WriteLine("MessageType from queue foo3 is : " + messageType2);
        }

        private static void SendMessage(string queue, string body)
        {
            Console.WriteLine("Opening connection ...");
            var connection = new Connection(new Address("localhost", 5672, "user", "pass", "/", "AMQP"));
            var session = new Session(connection);
            var target = new Target() { Durable = 1, Address = "/queue/" + queue };
            var sender = new SenderLink(session, "foo-sender", target, null);
            try
            {

                Console.WriteLine("Sending message ...");
                var amqpmessage = new Amqp.Message();
                amqpmessage.Properties = new Amqp.Framing.Properties() { Subject = "foo" };
                amqpmessage.Header = new Amqp.Framing.Header() { Durable = true };
                amqpmessage.BodySection = new Amqp.Framing.Data() { Binary = Encoding.UTF8.GetBytes(body) };
                amqpmessage.ApplicationProperties = new ApplicationProperties();
                amqpmessage.ApplicationProperties["messagetype"] = "foo";

                sender.Send(amqpmessage);
                Console.WriteLine("Message sent ...");
            }
            finally
            {
                sender.Close();
                session.Close();
                connection.Close();
            }
        }

        private static Message ReceiveMessage(string queue)
        {
            var connection = new Connection(new Address("localhost", 5672, "user", "pass", "/", "AMQP"));

            var session = new Amqp.Session(connection);

            Amqp.Framing.Attach rcvAttach = new Amqp.Framing.Attach()
            {
                Source = new Amqp.Framing.Source()
                {
                    Address = "/queue/" + queue,
                    Durable = 1
                },

                Target = new Amqp.Framing.Target()
                {
                    Durable = 1
                }
            };

            var receiver = new Amqp.ReceiverLink(session, "foo-receiver", rcvAttach, null);

            Amqp.Message amqpMessage = null;

            try
            {
                amqpMessage = receiver.Receive(new TimeSpan(0, 0, 5)); // wait - timeout 5 seconds

                if (amqpMessage == null)
                {
                    // nothing received - timeout expired
                    // queue is empty !
                    Console.WriteLine("No message received, queue is empty ...");
                    return null;
                }
                else
                {
                    receiver.Accept(amqpMessage);
                    return amqpMessage;
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
    }
}
