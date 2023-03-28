DITTO_DEVOPS_PWD=$(kubectl --namespace cloud get secret c2e-ditto-gateway-secret -o jsonpath="{.data.devops-password}" | base64 --decode)
DITTO_IP=127.0.0.1
DITTO_PORT=32230
KAFKA_CLUSTER=kafka-cluster-2:9092

curl -i -X POST -u devops:${DITTO_DEVOPS_PWD} -H 'Content-Type: application/json' --data '{
  "targetActorSelection": "/system/sharding/connection",
  "headers": {
    "aggregate": false
  },
  "piggybackCommand": {
    "type": "connectivity.commands:createConnection",
    "connection": {
        "id": "telegraf-kafka-connection",
        "connectionType": "kafka",
        "connectionStatus": "open",
        "failoverEnabled": true,
        "uri": "tcp://c2e-kafka-1:9092",
        "specificConfig": {
            "bootstrapServers": "c2e-kafka-1:9092",
            "saslMechanism": "plain"
        },
        "sources": [],
        "targets": [
            {
            "address": "digitaltwins",
            "topics": [
                "_/_/things/twin/events",
                "_/_/things/live/messages"
            ],
            "authorizationContext": [
                "nginx:ditto"
            ]
            }
        ]
    }
  }
}' http://$DITTO_IP:$DITTO_PORT/devops/piggyback/connectivity
