import json, os, subprocess, shutil

def create_config_and_run(clusters, replicas, shard_size):
    config = {}
    clusterNum = 1
    port = 8081
    server_num = 1
    for cluster in range(1, clusters+1):
        servers = []
        for replica in range(1, replicas+1):
            replica_dict = {"id": f"S{server_num}", "port": f":{port}"}
            servers.append(replica_dict)
            port += 1
            server_num += 1
        start = (cluster-1) * shard_size + 1
        end = (cluster) * shard_size
        config[f"C{cluster}"] = {'nodes': servers, 'start': f'{start}', 'end': f'{end}' }
    with open('config.json', 'w') as json_file:
        json.dump(config, json_file, indent=4)

    for cluster in config:
        for server in config[cluster]['nodes']:
            command = f"go run main.go --cluster-id={cluster} --id={server['id']} --logs"
            subprocess.Popen(["gnome-terminal", "--title", f"{cluster}.{server['id']}", "--", "bash", "-c", command])


if __name__ == "__main__":
    clusters = 3
    replicas = 4

    print("Resetting datastore")
    try:
        shutil.rmtree("./dbs")
    except Exception as e:
        pass
    os.makedirs("./dbs", exist_ok=True)

    shard_size = 3000 // clusters

    create_config_and_run(clusters, replicas, shard_size)