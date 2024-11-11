# Pub-Sub-Privacy

Built in GDPR compliance in a MQTT broker

## Description

Pub-Sub-Privacy is a customized MQTT broker (Eclipse Mosquitto) that explores enabling GDPR compliance in MQTT through the use of plugins. In this project we extend the Mosquitto broker to handle GDPR rights in the context of how they can be applied in MQTT.

## Getting Started

### Dependencies

- **Docker**: Ensure you have Docker installed .
- **Basic MQTT Knowledge**: Familiarity with MQTT protocol, MQTT brokers, and MQTT clients.
- **Operating System**: Compatible with any OS that support Docker.


### Installing

   1. **Clone the Repository**

      Clone the project repository to your local machine:

      ```
      git clone https://github.com/DAMSlabUMBC/pub-sub-privacy.git
      cd pub-sub-privacy/eclipse_mosquitto
      ```
   2. **Build the Custom Mosquitto Docker Image**

      Build the Docker image with the custom plugins included. The Dockerfile is configured 
      to compile the plugins and set up the broker.

      ```
      docker build -t mosquitto-custom .   
      ```
### Executing program
#### Run the GDPR-Compliant Mosquitto Broker

Start the Mosquitto broker with GDPR compliance built in
```
docker run -d --name gdpr-mosquitto-broker -p 1883:1883 mosquitto-custom
```
#### Verify the broker is running
```
docker ps
```
You should see ``` gdpr-mosquitto-broker``` listed as a running docker container
#### Connect to the Broker
Use any MQTT client in any language to publish and subscibe to topic via ```(your server):1883 ```
#### Sample GDPR Compliance
Follow our examples in the ```Examples``` folder for GDPR rights appliable in MQTT
## Help

If you encounter issues, consider the following troubleshooting steps:
* Check Docker Logs
  View the docker logs to find any errors
  ```
  docker logs gdpr-mosquitto-broker
  ```
* Acess the container shell to find errors
  ```
  docker exec -it gdpr-mosquitto-broker /bin/bash
  ```
* Rebuild the image 

## Version History

* 0.1
    * Initial Release

## License

This project is licensed under the MIT License - see the LICENSE.md file for details

## Acknowledgments

Inspiration, code snippets, etc.
* [awesome-readme](https://github.com/matiassingers/awesome-readme)
* [PurpleBooth](https://gist.github.com/PurpleBooth/109311bb0361f32d87a2)
* [dbader](https://github.com/dbader/readme-template)
* [zenorocha](https://gist.github.com/zenorocha/4526327)
* [fvcproductions](https://gist.github.com/fvcproductions/1bfc2d4aecb01a834b46)
