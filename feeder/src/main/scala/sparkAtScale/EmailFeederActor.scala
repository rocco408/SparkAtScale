package sparkAtScale

import akka.actor.{Actor, ActorLogging, Props}
import org.apache.kafka.clients.producer.{Callback, ProducerRecord, RecordMetadata}

import scala.concurrent.duration.{Duration, _}
import scala.util.Random
import java.util.UUID
import org.joda.time.DateTime

case class Email(
                  msg_id: String,
                  tenant_id: UUID,
                  mailbox_id: UUID,
                  time_delivered: Long,
                  time_forwarded: Long,
                  time_read: Long,
                  time_replied: Long) {

  override def toString: String = {
    s"$msg_id::$tenant_id::$mailbox_id::$time_delivered::$time_forwarded::$time_read::$time_replied"
  }
}

/**
 * Pull data from akka, saves to C* 
 */
class EmailFeederActor(tickInterval:FiniteDuration) extends Actor with ActorLogging with FeederExtensionActor {

  log.info(s"Starting the email feeder actor ${self.path}")

  import FeederActor.SendNextLine

  var emailsDelivered = 0
  var kafkaKeyCounter = 0

  implicit val executionContext = context.system.dispatcher

  val feederTick = context.system.scheduler.schedule(Duration.Zero, tickInterval, self, SendNextLine)

  val randGen = Random
  val dateTime = new DateTime(2000,1,1,0,0,0,0)

  var ratingsSent = 0
  var emailsSent = 0

  def receive = {
    case SendNextLine =>

      val numKafkaKeys = feederExtension.numKafkaKeys // the number of unique Kafka keys to generate, we want to constrain this to test mapWithState
      val nxtMessageId = f"messageId-tenant01-${kafkaKeyCounter}%06d" // ex. "messageId-tenant01-074652".getBytes.length == 25
      // follow-up needed: keeping this fixed for now to make querying easier
      val nxtTenantId = UUID.fromString("9b657ca1-bfb1-49c0-85f5-04b127adc6f3")
      val nxtMailboxId = UUID.randomUUID()
      val nxtTimeDelivered = dateTime.plusSeconds(randGen.nextInt()).getMillis
      val nxtTimeForwarded = dateTime.plusSeconds(randGen.nextInt()).getMillis
      val nxtTimeRead = dateTime.plusSeconds(randGen.nextInt()).getMillis
      val nxtTimeReplied = dateTime.plusSeconds(randGen.nextInt()).getMillis

      val nxtEmail = Email(nxtMessageId, nxtTenantId, nxtMailboxId, nxtTimeDelivered, nxtTimeForwarded, nxtTimeRead, nxtTimeReplied)
      emailsSent += 1

      //email data has the format msg_id:tenant_id:mailbox_id:time_delivered:time_forwarded:time_read:time_replied
      //the key for the producer record is (msg_id)
      val email_key = s"${nxtEmail.msg_id}"

      // using the kafkaKeyCounter to control which kafka partition the record is routed to, without it we und up using kafka's hashing
      // mechanism for routing records to kafka partitions and we have found the distribution to be non-uniform (some partitions end up with no records)
      val email_record = new ProducerRecord[String, String](feederExtension.kafkaTopic, kafkaKeyCounter, email_key, nxtEmail.toString)

      val email_future = feederExtension.producer.send(email_record, new Callback {
        override def onCompletion(result: RecordMetadata, exception: Exception) {
          if (exception != null) log.info("Failed to send email_record: " + exception)
          else {
            emailsDelivered += 1
            // Only output 5% because this output can get huge
            if (randGen.nextInt(100) < 5) {
              println(s"Number of emails successfully delivered to Kafka: $emailsDelivered msg_id: ${nxtEmail.msg_id} tenant_id: ${nxtEmail.tenant_id} mailbox_id: ${nxtEmail.mailbox_id}")
              //periodically log the num of messages sent
              //if (emailsSent % 20987 == 0)
              //  log.info(s"emailsSent = $emailsSent  //  result partition: ${result.partition()}")
            }
          }
        }
      })

      // cycle through the range of kafka keys instead of relying on a random number generator that may not populate topics
      if (kafkaKeyCounter == numKafkaKeys-1) {
        kafkaKeyCounter = 0
      } else {
        kafkaKeyCounter += 1
      }

    // Use future.get() to make this a synchronous write
  }
}

object EmailFeederActor {
  def props(tickInterval:FiniteDuration) = Props(new EmailFeederActor(tickInterval))
  case object ShutDown

  case object SendNextLine

}
