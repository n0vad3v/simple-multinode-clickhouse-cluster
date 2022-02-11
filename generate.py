import yaml
import os

def make_dir(distinct_host_list):
    prefix = "cluster/"
    for host in distinct_host_list:
        if not os.path.exists(prefix + host):
            os.makedirs(prefix + host)

def gen_zookeeper_server_env(zookeeper_host_list,current_host):

    general_env = ""
    for host in zookeeper_host_list:
        each_host = host + ":2888:3888,"
        general_env += each_host

    # remove last ","
    general_env = general_env[:-1]

    # Replace current host to 0.0.0.0
    general_env = general_env.replace(current_host,"0.0.0.0")

    return general_env

def gen_zookeeper_docker_compose_content(zookeeper_host_list,zookeeper_image,current_host):

    content = """
  zookeeper:
    image: {zookeeper_image}
    restart: always
    network_mode: "host"
    environment:
      - ZOO_SERVER_ID={zookeeper_server_id}
      - ALLOW_ANONYMOUS_LOGIN=yes
      - ZOO_SERVERS={zookeeper_server_env}
    """

    content = content.format(zookeeper_image=zookeeper_image,
    zookeeper_server_id = zookeeper_host_list.index(current_host) + 1,
    zookeeper_server_env = gen_zookeeper_server_env(zookeeper_host_list,current_host))

    return content

def gen_clickhouse_docker_compose_content(clickhouse_image):
    content = """
  clickhouse:
    image: {clickhouse_image}
    restart: always
    network_mode: "host"
    volumes:
      - ./event-data:/var/lib/clickhouse
      - ./clickhouse-config.xml:/etc/clickhouse-server/config.d/logging.xml:ro
      - ./clickhouse-user-config.xml:/etc/clickhouse-server/users.d/logging.xml:ro
    ulimits:
      nofile:
        soft: 262144
        hard: 262144
    """

    content = content.format(clickhouse_image=clickhouse_image)

    return content

def gen_global_docker_compose_content(distinct_host_list,zookeeper_host_list,clickhouse_host_list,zookeeper_image,clickhouse_image):
    prefix_content = """
version: '3'
services:
"""
    for host in distinct_host_list:
        content = prefix_content
        if host in zookeeper_host_list:
            content += gen_zookeeper_docker_compose_content(zookeeper_host_list,zookeeper_image,host)
        if host in clickhouse_host_list:
            content += gen_clickhouse_docker_compose_content(clickhouse_image)
            # copy ./sample/clickhouse-user-config.xml ./cluster/{host}/clickhouse-user-config.xml
            os.system("cp ./sample/clickhouse-user-config.xml ./cluster/" + host + "/clickhouse-user-config.xml")
        
        with open("./cluster/" + host + "/docker-compose.yml","w+") as f:
            f.write(content)
        
def gen_clickhouse_config_xml(zookeeper_host_list,clickhouse_host_list):
    for current_host in clickhouse_host_list:
        global_content = """
        <yandex>
            <logger>
                <level>warning</level>
                <console>true</console>
            </logger>

            <!-- Stop all the unnecessary logging -->
            <query_thread_log remove="remove"/>
            <query_log remove="remove"/>
            <text_log remove="remove"/>
            <trace_log remove="remove"/>
            <metric_log remove="remove"/>
            <asynchronous_metric_log remove="remove"/>

            <listen_host>0.0.0.0</listen_host>
            <http_port>8123</http_port>
            <tcp_port>9000</tcp_port>
            <interserver_http_host>{current_host}</interserver_http_host>
            <interserver_http_port>9009</interserver_http_port>

        {zookeeper_config}

        {remote_servers_config}

        <marcos>
            <cluster>novakwok_cluster</cluster>
            <shard>0</shard>
            <replica>{current_host}</replica>
        </marcos>
        </yandex>
        """

        ## Zookeeper config
        zookeeper_config = """
        <zookeeper>
        """

        for host in zookeeper_host_list:
            content = """
            <node index="{index}">
                <host>{host}</host>
                <port>2181</port>
            </node>
            """
            content = content.format(index = zookeeper_host_list.index(host) + 1,host = host)
            formated_content = content
            zookeeper_config += formated_content
        
        zookeeper_config = zookeeper_config + """
        </zookeeper>
        """
        ## Zookeeper config

        ## Remote servers config
        remote_servers_config = """
        <remote_servers>
            <novakwok_cluster>
                <shard>
        """

        for host in clickhouse_host_list:
            content = """
                    <replica>
                        <host>{host}</host>
                        <port>9000</port>
                    </replica>
            """
            content = content.format(host = host)
            formated_content = content
            remote_servers_config += formated_content
        
        remote_servers_config = remote_servers_config + """
                </shard>
            </novakwok_cluster>
        </remote_servers>
        """
        ## Remote servers config
        
        each_content = global_content.format(zookeeper_config = zookeeper_config,remote_servers_config = remote_servers_config,current_host = current_host)

        # Write clickhouse-config.xml to cluster/{host}/clickhouse-config.xml
        with open("./cluster/" + current_host + "/clickhouse-config.xml","w+") as f:
            print("Write clickhouse-config.xml to cluster/" + current_host + "/clickhouse-config.xml")
            f.write(each_content)

if __name__ == "__main__":
    config_file = "./topo.yml"

    with open(config_file, 'r') as stream:
        try:
            config = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)

    clickhouse_image = config['global']['clickhouse_image']
    zookeeper_image = config['global']['zookeeper_image']

    zookeeper_host_list = []
    zookeeper_hosts = config['zookeeper_servers']
    for host in zookeeper_hosts:
        zookeeper_host_list.append(host['host'])

    clickhouse_host_list = []
    clickhouse_hosts = config['clickhouse_servers']
    for host in clickhouse_hosts:
        clickhouse_host_list.append(host['host'])

    distinct_host_list = list(set(clickhouse_host_list + zookeeper_host_list))

    make_dir(distinct_host_list)
    gen_global_docker_compose_content(distinct_host_list,zookeeper_host_list,clickhouse_host_list,zookeeper_image,clickhouse_image)
    gen_clickhouse_config_xml(zookeeper_host_list,clickhouse_host_list)