#!/usr/bin/env python3

from mosq_test_helper import *
import mqtt5_rc

def do_test(conn, data):
    props = mqtt5_props.gen_uint32_prop(mqtt5_props.PROP_MAXIMUM_PACKET_SIZE, 1000)
    props += mqtt5_props.gen_uint16_prop(mqtt5_props.PROP_RECEIVE_MAXIMUM, 20)
    connect_packet = mosq_test.gen_connect("01-extended-auth", proto_ver=5, properties=props)

    props = mqtt5_props.gen_string_prop(mqtt5_props.PROP_AUTHENTICATION_METHOD, "test-method")
    props += mqtt5_props.gen_string_prop(mqtt5_props.PROP_AUTHENTICATION_DATA, "test-request") # This is really a binary property
    auth_continue_b2c = mosq_test.gen_auth(reason_code=mqtt5_rc.MQTT_RC_CONTINUE_AUTHENTICATION, properties=props)

    props = mqtt5_props.gen_string_prop(mqtt5_props.PROP_AUTHENTICATION_METHOD, "test-method")
    props += mqtt5_props.gen_string_prop(mqtt5_props.PROP_AUTHENTICATION_DATA, "test-reply") # This is really a binary property
    auth_continue_c2b = mosq_test.gen_auth(reason_code=mqtt5_rc.MQTT_RC_CONTINUE_AUTHENTICATION, properties=props)
    connack_packet = mosq_test.gen_connack(rc=0, proto_ver=5)

    disconnect_packet = mosq_test.gen_disconnect(proto_ver=5)

    mosq_test.do_receive_send(conn, connect_packet, auth_continue_b2c, "auth_b2c")
    mosq_test.do_receive_send(conn, auth_continue_c2b, connack_packet, "connack")
    mosq_test.expect_packet(conn, "disconnect", disconnect_packet)


mosq_test.client_test("c/01-extended-auth-continue.test", [], do_test, None)
mosq_test.client_test("cpp/01-extended-auth-continue.test", [], do_test, None)
