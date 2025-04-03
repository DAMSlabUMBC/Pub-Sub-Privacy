from mininet.net import Containernet
from mininet.node import Controller
from mininet.link import TCLink

def run_topology():
    net = Containernet(controller=Controller, link=TCLink)
    net.addController('c0')

    broker1 = net.addDocker(
        'bk1',
        dimage='dams-mosquitto',
        dcmd='/usr/local/sbin/mosquitto -c /etc/mosquitto/mosquitto.conf',
        ip='10.0.0.251',
        volumes=["/opt/mqtt_brokers/benchmark/mosquitto1.conf:/etc/mosquitto/mosquitto.conf"]
    )
    broker2 = net.addDocker(
        'bk2',
        dimage='dams-mosquitto',
        dcmd='/usr/local/sbin/mosquitto -c /etc/mosquitto/mosquitto.conf',
        ip='10.0.0.252',
        volumes=["/opt/mqtt_brokers/benchmark/mosquitto2.conf:/etc/mosquitto/mosquitto.conf"]
    )
    broker3 = net.addDocker(
        'bk3',
        dimage='dams-mosquitto',
        dcmd='/usr/local/sbin/mosquitto -c /etc/mosquitto/mosquitto.conf',
        ip='10.0.0.253',
        volumes=["/opt/mqtt_brokers/benchmark/mosquitto3.conf:/etc/mosquitto/mosquitto.conf"]
    )

    bench = net.addDocker(
        'bench',
        dimage='benchmark-runner',
        ip='10.0.0.10',
        volumes=["/opt/mqtt_brokers/benchmark:/benchmark"]
    )

    # link them all
    net.addLink(broker1, bench)
    net.addLink(broker2, bench)
    net.addLink(broker3, bench)

    net.start()

    # Retrieve the IP address of broker1
    broker1_ip = broker1.IP()

    # Then run the benchmarks on whichever one you want
    command = f"python3 /benchmark/Benchmark.py run /benchmark/benchmark-PM_1-1.cfg {broker1_ip}"
    print("Running benchmark with command: ", command)
    command1 = f"python3 /benchmark/Benchmark.py run /benchmark/benchmark-PM_1-2.cfg {broker1_ip}"
    print("Running benchmark with command: ", command1)
    print(bench.cmdPrint(command))
    print(bench.cmdPrint(command1))

    input("Press Enter to stop network...")
    net.stop()

if __name__ == '__main__':
    run_topology()
