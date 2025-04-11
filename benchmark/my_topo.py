from mininet.net import Containernet
from mininet.node import Controller
from mininet.link import TCLink

def run_topology():
    net = Containernet(controller=Controller, link=TCLink)
    net.addController('c0')

    # Create a switch
    s1 = net.addSwitch('s1')

    non_mod = net.addDocker(
        'non_mod',
        dimage='dams-mosquitto',
        dcmd='/usr/local/sbin/mosquitto -c /etc/mosquitto/mosquitto.conf',
        ip='10.0.0.251',
        network_mode='none',
        dpriv=True,
        volumes=["/opt/mqtt_brokers/benchmark/mosquitto-non-modifying.conf:/etc/mosquitto/mosquitto.conf"]
    )
    per_msg = net.addDocker(
        'per_msg',
        dimage='dams-mosquitto',
        dcmd='/usr/local/sbin/mosquitto -c /etc/mosquitto/mosquitto.conf',
        ip='10.0.0.251',
        network_mode='none',
        dpriv=True,
        volumes=["/opt/mqtt_brokers/benchmark/mosquitto-per-message.conf:/etc/mosquitto/mosquitto.conf"]
    )
    reg_by_msg = net.addDocker(
        'reg_by_msg',
        dimage='dams-mosquitto',
        dcmd='/usr/local/sbin/mosquitto -c /etc/mosquitto/mosquitto.conf',
        ip='10.0.0.252',
        network_mode='none',
        dpriv=True,
        volumes=["/opt/mqtt_brokers/benchmark/mosquitto-reg-by-message.conf:/etc/mosquitto/mosquitto.conf"]
    )
    reg_by_topic = net.addDocker(
        'reg_by_topic',
        dimage='dams-mosquitto',
        dcmd='/usr/local/sbin/mosquitto -c /etc/mosquitto/mosquitto.conf',
        ip='10.0.0.253',
        network_mode='none',
        dpriv=True,
        volumes=["/opt/mqtt_brokers/benchmark/mosquitto-reg-by-topic.conf:/etc/mosquitto/mosquitto.conf"]
    )

    bench = net.addDocker(
        'bench',
        dimage='benchmark-runner',
        ip='10.0.0.10',
        network_mode='none',
        dpriv=True,
        volumes=["/opt/mqtt_brokers/benchmark:/benchmark"]
    )

    # link them all
    net.addLink(non_mod, s1,
    intfName1='nm0')
    net.addLink(per_msg, s1,
    intfName1='pm0')
    net.addLink(reg_by_msg, s1,
    intfName1='rbm0')
    net.addLink(reg_by_topic, s1,
    intfName1='rbtp0')
    net.addLink(bench, s1,
    intfName1='b0')

    net.start()
    
    print(per_msg.cmd("ip link show"))
    print(per_msg.cmd("ip addr show"))
    
    # Force pm0 up in the per_msg container
    non_mod.cmd("ip link set dev nm0 up")
    per_msg.cmd("ip link set dev pm0 up")
    reg_by_msg.cmd("ip link set dev rbm0 up")
    reg_by_topic.cmd("ip link set dev rbtp0 up")
    bench.cmd("ip link set dev b0 up")


    # Now check again
    print(non_mod.cmd("ip addr show pm0"))
    print(per_msg.cmd("ip addr show pm0"))
    print(reg_by_msg.cmd("ip addr show rbm0"))
    print(reg_by_topic.cmd("ip addr show rbtp0"))



    # Retrieve the IP address of broker1
    non_mod_ip = non_mod.IP()
    per_msg_ip = per_msg.IP()
    reg_by_msg_ip = reg_by_msg.IP()
    reg_by_topic_ip = reg_by_topic.IP()

    

    # Then run the benchmarks on whichever one you want
    command0 = f"python3 /benchmark/Benchmark.py run /benchmark/configs/examples/B0.cfg {non_mod_ip} --logfile /benchmark/logs/B0_run.log"
    print("Running benchmark with command: ", command0)
    print(bench.cmdPrint(command0))
    command = f"python3 /benchmark/Benchmark.py run /benchmark/configs/examples/B1.cfg {per_msg_ip} --logfile /benchmark/logs/B1_run.log"
    print("Running benchmark with command: ", command)
    print(bench.cmdPrint(command))
    command1 = f"python3 /benchmark/Benchmark.py run /benchmark/configs/examples/B2.cfg {reg_by_msg_ip} --logfile /benchmark/logs/B2_run.log"
    print("Running benchmark with command: ", command1)
    print(bench.cmdPrint(command1))
    command2 = f"python3 /benchmark/Benchmark.py run /benchmark/configs/examples/B3.cfg {reg_by_topic_ip}  --logfile /benchmark/logs/B3_run.log"
    print("Running benchmark with command: ", command2)
    print(bench.cmdPrint(command2))

    input("Press Enter to stop network...")
    net.stop()

if __name__ == '__main__':
    run_topology()

