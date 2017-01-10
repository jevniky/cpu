import paho.mqtt.client as mqtt
import MySQLdb
# Open database connection
db = MySQLdb.connect("localhost","partizan","peterko16000","homeiot" )
# prepare a cursor object using cursor() method
cursor = db.cursor()

debug = 1

# TODO: Create database and table;

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))
    # Subscribe
    client.subscribe("+/state")
    mqtt_client.subscribe("+/info/#")
    # publish that CPU is ready
    client.publish("cpu/state", "1", 2, True)

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    if debug:
        print(msg.topic+" "+str(msg.payload))
    # topic split using "/"
    topic_split = msg.topic.split('/')
    # NOTE topic_split[0] = device SN
    # NOTE topic_split[1] = "state" / "info" / "output"
    # NOTE topic_split[2] = "ip" / doesn't exist

    # NOTE just to get rid of the magic number as an index in the list
    device_sn = topic_split[0]
    # is the device online?
    if "state" == topic_split[1] and "1" == str(msg.payload) and "cpu" != str(topic_split[0]): # if the device is online and connected and it's other device than CPU:
	# Save the SN of the device
        if debug:
            print("New device! "+device_sn)
        # # Gather thermometers
	    # if "TEMP" in device_sn: # If the device is a thermometer
        #     device_type = "Thermometer"
        #     thermometers.append(device_sn) # save it's SN
        sql = "INSERT INTO devices(`sn`, `online`) VALUES ('%s', %d) WHERE NOT EXISTS (SELECT `sn` FROM `devices` WHERE `sn` = '%s')" % (device_sn, 1, device_sn)
        try:
            cursor.execute(sql) # Execute the SQL command
            db.commit() # Commit your changes in the database
        except MySQLdb.Error, e:
            try:
                print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
            except IndexError:
                print "MySQL Error: %s" % str(e)
    # Device has disconnected
    elif "state" == topic_split[1] and "0" == str(msg.payload): # if the device has disconnected:
        if debug:
            print("Offline device! "+device_sn)
        # When the device disconnects - set 0 as state in devices table and clear IP address
        sql = "UPDATE `devices` SET `state` = %d, `ip` = 'N/A' WHERE `sn` = '%s'" % (0, device_sn)
        try:
            cursor.execute(sql) # Execute the SQL command
            db.commit() # Commit your changes in the database
        except MySQLdb.Error, e:
            try:
                print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
            except IndexError:
                print "MySQL Error: %s" % str(e)

    elif "info" == topic_split[1]:
        if "ip" == topic_split[2]:
            sql = "UPDATE `devices` SET `ip` = %s WHERE `sn` = '%s'" % (str(msg.payload), device_sn)
            try:
                cursor.execute(sql) # Execute the SQL command
                db.commit() # Commit your changes in the database
            except MySQLdb.Error, e:
                try:
                    print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
                except IndexError:
                    print "MySQL Error: %s" % str(e)
        if "type" == topic_split[2]:
            sql = "UPDATE `devices` SET `type` = %s WHERE `sn` = '%s'" % (str(msg.payload), device_sn)
            try:
                cursor.execute(sql) # Execute the SQL command
                db.commit() # Commit your changes in the database
            except MySQLdb.Error, e:
                try:
                    print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
                except IndexError:
                    print "MySQL Error: %s" % str(e)
    else:
        if debug:
            print("Topic "+topic_split[1]+" is a wrong topic!")
        # TODO What to do if the topic is wrong? Just ignore, right?

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.connect("192.168.0.12", 1883, 60)
# will_set(topic, payload, qos, retain)
client.will_set("cpu/state", "0", 2, True)

# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
# Other loop*() functions are available that give a threaded interface and a
# manual interface.
client.loop_forever()
