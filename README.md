# Simple Multi Node Clickhouse Cluster

I hate those single-node clickhouse clusters(Running multiple clickhouse instance inside one docker-compose, this is just weird!), setting up a multi-node clickhouse cluster seems not very handy and may require lots of manual work, so this repo tries to solve this problem.

> Note: This is a simplified model of Multi Node Clickhouse Cluster, which lacks: LoadBalancer config/Automated Failover/MultiShard Config generation.

## Prerequisites

To use this, we need `docker` and `docker-compose` installed, recommended OS is `ubuntu`, and it's recommended to install `clickhouse-client` on machine, so on a typical ubuntu server, doing the following should be sufficient.

```bash
apt update
curl -fsSL https://get.docker.com -o get-docker.sh && sh get-docker.sh && rm -f get-docker.sh
apt install docker-compose clickhouse-client -y
```

## Usage

1. Clone this repo
2. Edit the necessary server info in `topo.yml`
3. Run `python3 generate.py`
4. Your cluster info should be in the `cluster` directory now
5. Sync those files to related nodes and run `docker-compose up -d` on them
6. Your cluster is ready to go


If you still cannot understand what I'm saying above, see the example below.

## Example Usage

### Edit information

I've Clone the repo and would like to set a 3-master clickhouse cluster and has the following specs

* 3 replica(one replica on each node)
* 1 Shard only

![](./demo-cluster.png)

So I need to edit the `topo.yml` as follows:

```yml
global:
  clickhouse_image: "yandex/clickhouse-server:21.3.2.5"
  zookeeper_image: "bitnami/zookeeper:3.6.1"

zookeeper_servers:
  - host: 192.168.33.101
  - host: 192.168.33.102
  - host: 192.168.33.103

clickhouse_servers:
  - host: 192.168.33.101
  - host: 192.168.33.102
  - host: 192.168.33.103

clickhouse_topology:
  - clusters:
      - name: "novakwok_cluster"
        shards:
          - name: "novakwok_shard"
            servers:
              - host: 192.168.33.101
              - host: 192.168.33.102
              - host: 192.168.33.103
```

### Generate Config

After `python3 generate.py`, a structure has been generated under `cluster` directory, looks like this:

```
➜  simple-multinode-clickhouse-cluster git:(master) ✗ python3 generate.py 
Write clickhouse-config.xml to cluster/192.168.33.101/clickhouse-config.xml
Write clickhouse-config.xml to cluster/192.168.33.102/clickhouse-config.xml
Write clickhouse-config.xml to cluster/192.168.33.103/clickhouse-config.xml

➜  simple-multinode-clickhouse-cluster git:(master) ✗ tree cluster 
cluster
├── 192.168.33.101
│   ├── clickhouse-config.xml
│   ├── clickhouse-user-config.xml
│   └── docker-compose.yml
├── 192.168.33.102
│   ├── clickhouse-config.xml
│   ├── clickhouse-user-config.xml
│   └── docker-compose.yml
└── 192.168.33.103
    ├── clickhouse-config.xml
    ├── clickhouse-user-config.xml
    └── docker-compose.yml

3 directories, 9 files
```

### Sync Config

Now we need to sync those files to related hosts(of course you can use ansible here):

```
rsync -aP ./cluster/192.168.33.101/ root@192.168.33.101:/root/ch/
rsync -aP ./cluster/192.168.33.102/ root@192.168.33.102:/root/ch/
rsync -aP ./cluster/192.168.33.103/ root@192.168.33.103:/root/ch/
```

### Start Cluster

Now run `docker-compose up -d` on every hosts' `/root/ch/` directory.

### Validation

On 192.168.33.101, use `clickhouse-client` to connect to local instance and check if cluster is there.

```
root@192-168-33-101:~/ch# clickhouse-client 
ClickHouse client version 18.16.1.
Connecting to localhost:9000.
Connected to ClickHouse server version 21.3.2 revision 54447.

192-168-33-101 :) SELECT * FROM system.clusters;

SELECT *
FROM system.clusters 

┌─cluster──────────────────────────────────────┬─shard_num─┬─shard_weight─┬─replica_num─┬─host_name──────┬─host_address───┬─port─┬─is_local─┬─user────┬─default_database─┬─errors_count─┬─estimated_recovery_time─┐
│ novakwok_cluster                             │         1 │            1 │           1 │ 192.168.33.101 │ 192.168.33.101 │ 9000 │        1 │ default │                  │            0 │                       0 │
│ novakwok_cluster                             │         1 │            1 │           2 │ 192.168.33.102 │ 192.168.33.102 │ 9000 │        0 │ default │                  │            0 │                       0 │
│ novakwok_cluster                             │         1 │            1 │           3 │ 192.168.33.103 │ 192.168.33.103 │ 9000 │        0 │ default │                  │            0 │                       0 │
│ test_cluster_two_shards                      │         1 │            1 │           1 │ 127.0.0.1      │ 127.0.0.1      │ 9000 │        1 │ default │                  │            0 │                       0 │
│ test_cluster_two_shards                      │         2 │            1 │           1 │ 127.0.0.2      │ 127.0.0.2      │ 9000 │        0 │ default │                  │            0 │                       0 │
│ test_cluster_two_shards_internal_replication │         1 │            1 │           1 │ 127.0.0.1      │ 127.0.0.1      │ 9000 │        1 │ default │                  │            0 │                       0 │
│ test_cluster_two_shards_internal_replication │         2 │            1 │           1 │ 127.0.0.2      │ 127.0.0.2      │ 9000 │        0 │ default │                  │            0 │                       0 │
│ test_cluster_two_shards_localhost            │         1 │            1 │           1 │ localhost      │ 127.0.0.1      │ 9000 │        1 │ default │                  │            0 │                       0 │
│ test_cluster_two_shards_localhost            │         2 │            1 │           1 │ localhost      │ 127.0.0.1      │ 9000 │        1 │ default │                  │            0 │                       0 │
│ test_shard_localhost                         │         1 │            1 │           1 │ localhost      │ 127.0.0.1      │ 9000 │        1 │ default │                  │            0 │                       0 │
│ test_shard_localhost_secure                  │         1 │            1 │           1 │ localhost      │ 127.0.0.1      │ 9440 │        0 │ default │                  │            0 │                       0 │
│ test_unavailable_shard                       │         1 │            1 │           1 │ localhost      │ 127.0.0.1      │ 9000 │        1 │ default │                  │            0 │                       0 │
│ test_unavailable_shard                       │         2 │            1 │           1 │ localhost      │ 127.0.0.1      │    1 │        0 │ default │                  │            0 │                       0 │
└──────────────────────────────────────────────┴───────────┴──────────────┴─────────────┴────────────────┴────────────────┴──────┴──────────┴─────────┴──────────────────┴──────────────┴─────────────────────────┘
↘ Progress: 13.00 rows, 1.58 KB (4.39 thousand rows/s., 532.47 KB/s.) 
13 rows in set. Elapsed: 0.003 sec. 
```

Let's create a DB with replica:

```
192-168-33-101 :) create database novakwok_test on cluster novakwok_cluster;

CREATE DATABASE novakwok_test ON CLUSTER novakwok_cluster

┌─host───────────┬─port─┬─status─┬─error─┬─num_hosts_remaining─┬─num_hosts_active─┐
│ 192.168.33.103 │ 9000 │      0 │       │                   2 │                0 │
│ 192.168.33.101 │ 9000 │      0 │       │                   1 │                0 │
│ 192.168.33.102 │ 9000 │      0 │       │                   0 │                0 │
└────────────────┴──────┴────────┴───────┴─────────────────────┴──────────────────┘
← Progress: 3.00 rows, 174.00 B (16.07 rows/s., 931.99 B/s.)  99%
3 rows in set. Elapsed: 0.187 sec. 

192-168-33-101 :) show databases;

SHOW DATABASES

┌─name──────────┐
│ default       │
│ novakwok_test │
│ system        │
└───────────────┘
↑ Progress: 3.00 rows, 479.00 B (855.61 rows/s., 136.61 KB/s.) 
3 rows in set. Elapsed: 0.004 sec. 

```

Connect to another host to see if it's really working.

```
root@192-168-33-101:~/ch# clickhouse-client -h 192.168.33.102
ClickHouse client version 18.16.1.
Connecting to 192.168.33.102:9000.
Connected to ClickHouse server version 21.3.2 revision 54447.

192-168-33-102 :) show databases;

SHOW DATABASES

┌─name──────────┐
│ default       │
│ novakwok_test │
│ system        │
└───────────────┘
↘ Progress: 3.00 rows, 479.00 B (623.17 rows/s., 99.50 KB/s.) 
3 rows in set. Elapsed: 0.005 sec. 

```