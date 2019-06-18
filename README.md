# Kafka Transport Encryption PoC

This project uses VS Code remote containers **[VS Code Remote - Containers](https://aka.ms/vscode-remote/containers)** extension.

## Setting up the development container

Follow these steps to open this sample in a container:

1. If this is your first time using a development container, please follow the [getting started steps](https://aka.ms/vscode-remote/containers/getting-started).

2. If you're not yet in a development container:
   - Clone this repository.
   - Press <kbd>F1</kbd> and select the **Remote-Containers: Open Folder in Container...** command.
   - Select the cloned copy of this folder, wait for the container to start, and try things out!

## Running

`mvn exec:java -Dexec.args="EVENTHUB_SAHRED_ACCESS_KEY TOPIC AD_CLIENT_ID AD_SECRET EVENT_HUB"`

