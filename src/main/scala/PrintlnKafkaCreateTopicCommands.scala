/**
 *
 *
 * @author Jev Prentice
 * @since 29 October 2021
 */
object PrintlnKafkaCreateTopicCommands extends App {
  List(
    //      "orders-by-user",
    "discount-profiles-by-user",
    //      "discounts",
    "orders",
    "payments",
    "paid-orders"
  ).foreach { topic => println(s"kafka-topics --bootstrap-server localhost:9092 --topic $topic --create") }
  // KTable: --config "cleanup.policy=compact"
  println("kafka-topics --bootstrap-server localhost:9092 --topic orders-by-user --create --config \"cleanup.policy=compact\"")
  println("kafka-topics --bootstrap-server localhost:9092 --topic discounts --create --config \"cleanup.policy=compact\"")
}
