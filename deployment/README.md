# Deployment of digitaltwins platform

## Pre-Requisites

Before you start, make sure you have access to k8s cluster.

Add repos:
```bash
helm repo add eclipse-iot https://eclipse.org/packages/charts
helm repo add influxdata https://helm.influxdata.com/
helm repo add grafana https://grafana.github.io/helm-charts
helm repo update
```

## Deploy Cloud2Edge

[Cloud2Edge](https://www.eclipse.org/packages/packages/cloud2edge/) is the core of digtwin platform, which contains [Eclipse Ditto](https://www.eclipse.org/ditto/) and [Eclipse Hono](https://www.eclipse.org/hono/).

Run:
```bash
export NS=cloud
kubectl create namespace $NS
RELEASE=c2e
helm install -n $NS --wait -f cloud2edge/cloud2edge-values.yaml $RELEASE eclipse-iot/cloud2edge
```

> Optional: deploy mongo express to view data stored in mongodb pod
> Run:
> ```bash
> kubectl apply -f cloud2edge/deploy-mongo-express.yaml -n $NS
> kubectl apply -f cloud2edge/svc-mongo-express.yaml -n $NS
> ```

## Deploy Kafka Cluster

Kafka cluster is used to connect Ditto and Telegraf. We use a new pod instead of the existing kafka pod in cloud2edge service because we tend to keep the integrity and independency of the service.

Run:
```bash
kubectl apply -f kafka-cluster/pod-kafka-1.yaml -n $NS
kubectl apply -f kafka-cluster/svc-kafka-1.yaml -n $NS
kubectl apply -f kafka-cluster/pod-zookeeper-1.yaml -n $NS
kubectl apply -f kafka-cluster/svc-zookeeper-1.yaml -n $NS
kubectl apply -f kafka-cluster/deploy-kafka-manager.yaml -n $NS
kubectl apply -f kafka-cluster/svc-kafka-manager.yaml -n $NS
```

After the installation is done, you should create a cluster with topic `digitaltwins` on deployed zookeeper.

Replace env variables in [connect-telegraf](cloud2edge/connect-telegraf.sh), and run:
```bash
bash cloud2edge/connect-telegraf.sh
```
This will creates a connection between Ditto and Kafka Cluster through ditto-connectivity service.

## Deploy Database

There are three types of database available to use, namely [IoTDB](https://iotdb.apache.org/), [InfluxDB2](https://www.influxdata.com/), [OpenTSDB](http://opentsdb.net/).

### InfluxDB2

Run:
```bash
kubectl apply -f influxdb2/sc-influxdb2.yaml -n $NS
kubectl apply -f influxdb2/pv-influxdb2.yaml -n $NS
helm install $RELEASE-influxdb2 influxdata/influxdb2 -f influxdb2/influxdb2-values.yaml -n $NS
```

### IoTDB

Run:
```bash
kubectl apply -f iotdb/pv-iotdb.yaml -n $NS
kubectl apply -f iotdb/pvc-iotdb.yaml -n $NS
kubectl apply -f iotdb/deploy-iotdb.yaml -n $NS
kubectl apply -f iotdb/svc-iotdb.yaml -n $NS
```

### OpenTSDB

Run:
```bash
kubectl apply -f opentsdb/deploy-opentsdb.yaml -n $NS
kubectl apply -f opentsdb/svc-opentsdb.yaml -n $NS
```

## Deploy Telegraf

[Telegraf](https://www.influxdata.com/time-series-platform/telegraf/) serves as a middleware to consume data from ditto and produce messages to database backends. You can specify database to use in [telegraf-values.yaml](telegraf-values.yaml).

If you want to deploy InfluxDB, please generate an access token in advance and replace the token in `telegraf-values.yaml` to your token before deployment.

Run:
```bash
helm install $RELEASE-telegraf -n $NS -f telegraf-values.yaml --set tplVersion=2 influxdata/telegraf
```

## Deploy Grafana

[Grafana](https://grafana.com/) provides operational dashboards to visualize data from source database. It uses 3D models from Unity to visualize data collected from hono devices.

Run:
```bash
kubectl apply -f grafana/sc-grafana.yaml -n $NS
kubectl apply -f grafana/pv-grafana.yaml -n $NS
helm install $RELEASE-grafana -f deployment/grafana-values.yaml grafana/grafana
```

You need to install plugins under host path `/opentwins/grafana/plugins` manually.

+ IoTDB plugin: [_v1.0.0_](https://github.com/apache/iotdb/releases/download/v1.0.0/apache-iotdb-1.0.0-grafana-plugin-bin.zip) [_Source_](https://github.com/apache/iotdb)
+ Unity plugin: [_Source_](https://github.com/ertis-research/unity-plugin-for-grafana)

After installing plugins, you need to configure data sources under grafana dashboard.
