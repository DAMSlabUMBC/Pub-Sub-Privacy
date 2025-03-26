from mininet.net import Containernet
from mininet.node import Controller
from mininet.link import TCLink

def run_topology():
    net = Containernet(controller=Controller, link=TCLink)
    net.addController('c0')

    broker1 = net.addDocker(
        'bk1',
        dimage='dams-mosquitto',
        volumes=["/opt/mqtt_brokers/benchmark/mosquitto1.conf:/etc/mosquitto/mosquitto.conf"]
    )
    broker2 = net.addDocker(
        'bk2',
        dimage='dams-mosquitto',
        volumes=["/opt/mqtt_brokers/benchmark/mosquitto2.conf:/etc/mosquitto/mosquitto.conf"]
    )
    broker3 = net.addDocker(
        'bk3',
        dimage='dams-mosquitto',
        volumes=["/opt/mqtt_brokers/benchmark/mosquitto3.conf:/etc/mosquitto/mosquitto.conf"]
    )

    bench = net.addDocker(
        'bench',
        dimage='benchmark-runner'
    )

    # link them all
    net.addLink(broker1, bench)
    net.addLink(broker2, bench)
    net.addLink(broker3, bench)

    net.start()

    # Optionally test each
    bench.cmdPrint("ping -c3 bk1")
    bench.cmdPrint("ping -c3 bk2")
    bench.cmdPrint("ping -c3 bk3")

    # Then run the benchmark on whichever one you want
    bench.cmdPrint("python3 TestManagementModule.py --config benchmark-1.cfg --host bk1 --port 1883 --log /tmp/bk1.log")
    # Other ones
    # bench.cmdPrint("python3 TestManagementModule.py --config benchmark-2.cfg --host bk2 --port 1883 --log /tmp/bk2.log")
    # bench.cmdPrint("python3 TestManagementModule.py --config benchmark-3.cfg --host bk3 --port 1883 --log /tmp/bk3.log")

    input("Press Enter to stop network...")
    net.stop()

if __name__ == '__main__':
    run_topology()
