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

      ```bash
      git clone https://github.com/DAMSlabUMBC/pub-sub-privacy.git
      cd pub-sub-privacy/eclipse_mosquitto
   2. **Build the Custom Mosquitto Docker Image**

      Build the Docker image with the custom plugins included. The Dockerfile is configured to 
      compile the plugins and set up the broker.

      ```bash
      docker build -t mosquitto-custom .   
  
### Executing program

* Run the GDPR-Compliant Mosquitto Broker
* Step-by-step bullets
```
docker run -d --name gdpr-mosquitto-broker -p 1883:1883 mosquitto-custom

```

## Help

Any advise for common problems or issues.
```
command to run if program contains helper info
```

## Authors

Contributors names and contact info

ex. Dominique Pizzie  
ex. [@DomPizzie](https://twitter.com/dompizzie)

## Version History

* 0.2
    * Various bug fixes and optimizations
    * See [commit change]() or See [release history]()
* 0.1
    * Initial Release

## License

This project is licensed under the [NAME HERE] License - see the LICENSE.md file for details

## Acknowledgments

Inspiration, code snippets, etc.
* [awesome-readme](https://github.com/matiassingers/awesome-readme)
* [PurpleBooth](https://gist.github.com/PurpleBooth/109311bb0361f32d87a2)
* [dbader](https://github.com/dbader/readme-template)
* [zenorocha](https://gist.github.com/zenorocha/4526327)
* [fvcproductions](https://gist.github.com/fvcproductions/1bfc2d4aecb01a834b46)
