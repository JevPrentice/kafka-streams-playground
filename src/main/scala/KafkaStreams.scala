
// io.circe.generic.auto._ uses macros!

import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._
import io.circe.{Decoder, Encoder}
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.kstream.{GlobalKTable, JoinWindows}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream.{KStream, KTable}
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.scala.serialization.Serdes._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

import java.time.Duration
import java.time.temporal.ChronoUnit
import java.util.Properties

/**
 *
 *
 * @author Jev Prentice
 * @since 29 October 2021
 */
object KafkaStreams {

  private implicit def serde[A >: Null : Decoder : Encoder]: Serde[A] = {
    val serializer = (order: A) => order.asJson.noSpaces.getBytes()
    val deserializer = (bytes: Array[Byte]) => {
      val string = new String(bytes)
      //decode[A](string).toOption
      val aOrError = decode[A](string)
      aOrError match {
        case Right(a) => Option(a)
        case Left(error) =>
          println(s"There was an error converting the message $aOrError, $error")
          Option.empty
      }
    }
    Serdes.fromFn[A](serializer, deserializer)
  }

  object Domain {
    type UserId = String
    type Profile = String
    type Product = String
    type OrderId = String
    type Status = String

    //never use double for money!
    case class Order(orderId: OrderId, user: UserId, products: List[Product], amount: Double)

    case class Discount(profile: Profile, amount: Double)

    case class Payment(orderId: OrderId, status: String)
  }

  object Topics {
    val OrderByUser = "orders-by-user"
    val DiscountProfilesByUser = "discount-profiles-by-user"
    val Discounts = "discounts"
    val Orders = "orders"
    val Payments = "payments"
    val PaidOrders = "paid-orders"
  }

  // source = emits elements
  // flow = transforms elements along the way (e.g. map)
  // sink = "ingest" elements

  import Domain._
  import Topics._

  def main(args: Array[String]): Unit = {

    // topology
    val builder = new StreamsBuilder()

    // KStream: a linear flow of elements from a kafka topic
    val usersOrdersStream: KStream[UserId, Order]
    = builder.stream[UserId, Order](OrderByUser)

    // KTable (distributed): elements that flow through a table are maintained inside the broker
    val userProfilesTable: KTable[UserId, Profile]
    = builder.table[UserId, Profile](DiscountProfilesByUser)

    // GlobalKTable: copied to all the nodes in the cluster
    // useful for performance improvements but
    // be careful not to put too many items inside
    val discountProfilesGTable: GlobalKTable[Profile, Discount]
    = builder.globalTable[Profile, Discount](Discounts)

    // KStream transformation: filter, map, mapValues, flatMap, flatMapValues
    val expensiveOrders = usersOrdersStream.filter { (userId, order) =>
      order.amount > 1000
    }

    val listsOfProducts = usersOrdersStream.mapValues { order =>
      order.products
    }

    val productsStream = usersOrdersStream.flatMapValues(_.products)

    // join
    val ordersWithUserProfile = usersOrdersStream.join(userProfilesTable) { (order, profile) =>
      (order, profile)
    }

    val discountedOrdersStream = ordersWithUserProfile.join(discountProfilesGTable)(
      { case (userId, (orderId, profile)) => profile }, // key of the join - picked from the "left" stream
      { case ((order, profile), discount) => order.copy(amount = order.amount - discount.amount) } // values of the marched records
    )

    // pick another identifier
    val ordersStream = discountedOrdersStream.selectKey((userId, order) => order.orderId)
    val paymentsStream = builder.stream[OrderId, Payment](Payments)

    val joinWindow = JoinWindows.of(Duration.of(5, ChronoUnit.MINUTES))
    val joinOrdersPayments = (order: Order, payment: Payment) =>
      if (payment.status == "PAID") Option(order) else Option.empty[Order]
    val ordersPaid = ordersStream
      .join(paymentsStream)(joinOrdersPayments, joinWindow)
      //.filter((orderId, maybeOrder) => maybeOrder.isDefined)
      .flatMapValues(maybeOrders => maybeOrders.toIterable)

    // sink
    ordersPaid.to(PaidOrders)

    val topology = builder.build()

    val props = new Properties
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "orders-application")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.stringSerde.getClass)

    println(topology.describe())

    val application = new KafkaStreams(topology, props)
    application.start()

    println(application.state())
  }
}
