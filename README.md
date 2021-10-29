This project is a sample playground for interacting with kafka streams is based
on https://blog.rockthejvm.com/kafka-streams/

### Start up environment:

```
docker compose up

```

### Destroy environment:

```
docker compose down --remove-orphans
```

### Create bash session inside kafka

```
docker exec -it broker bash
```

```
docker exec -it broker bash

kafka-console-producer \
    --topic discounts \
    --broker-list localhost:9092 \
    --property parse.key=true \
    --property key.separator=,
profile1,{"profile":"profile1","amount":0.5 }
profile2,{"profile":"profile2","amount":0.25 }
profile3,{"profile":"profile3","amount":0.15 }

kafka-console-producer \
    --topic discount-profiles-by-user \
    --broker-list localhost:9092 \
    --property parse.key=true \
    --property key.separator=,
Daniel,profile1
Riccardo,profile2 

kafka-console-producer \
    --topic orders-by-user \
    --broker-list localhost:9092 \
    --property parse.key=true \
    --property key.separator=,
Daniel,{"orderId":"order1","user":"Daniel","products":[ "iPhone 13","MacBook Pro 15"],"amount":4000.0 }
Riccardo,{"orderId":"order2","user":"Riccardo","products":["iPhone 11"],"amount":800.0}

kafka-console-producer \
    --topic payments \
    --broker-list localhost:9092 \
    --property parse.key=true \
    --property key.separator=,
order1,{"orderId":"order1","status":"PAID"}
order2,{"orderId":"order2","status":"PENDING"}

kafka-console-consumer \
     --bootstrap-server localhost:9092 \
     --topic paid-orders \
     --from-beginning
```  