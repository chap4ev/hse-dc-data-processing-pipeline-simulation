import yaml


def get_config_from_yaml(file_name="config.yaml"):
    with open("config.yaml") as f:
        config_dict = yaml.safe_load(f.read())
    return config_dict


def preprocess_config(config_dict):
    config_dict["conf"] = {
        key.replace("-", "."): value
        for key, value in config_dict["cfg"].items()
    }
    return config_dict


config = get_config_from_yaml()
kafka_consumer_config = preprocess_config(config["kafka-consumer"])
greenplum_config = config["greenplum"]


if __name__ == "__main__":
    print("Kafka config: ", kafka_consumer_config)
    print("Greenplum config: ", greenplum_config)
