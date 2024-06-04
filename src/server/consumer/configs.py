import yaml


def get_config_from_yaml(file_name="config.yaml"):
    with open("config.yaml") as f:
        config_dict = yaml.safe_load(f.read())
    return config_dict


def preprocess_config(config_dict):
    old_keys = config_dict.keys()
    for key in old_keys:
        new_key = key.replace("-", ".")
        config_dict[new_key] = config_dict.pop(key)
    return config_dict


config = get_config_from_yaml()
kafka_consumer_config = preprocess_config(config["kafka-consumer"])
greenplum_config = config["greenplum"]


if __name__ == "__main__":
    print("Kafka config: ", kafka_consumer_config)
    print("Greenplum config: ", greenplum_config)
