# mqtt_v1

The source code for the mqtt feature for pilight can be downloaded here.

To get mqtt integrated with pilight, you need to copy or move several files from the download folder to different pilight folders and modify one lua core file:

⦁	copy or move mqtt.c and mqtt.h from mqtt_v1/core/ to pilight/libs/pilight/core/ 

⦁	copy or move mqtt.c and mqtt.h from mqtt_v1/lua_c/ to pilight/libs/pilight/lua_c/network/ 

⦁	Make a backup copy of pilight/libs/pilight/lua_c/network.h/ so you can restore it if installation would fail.

⦁	copy or move network.h from mqtt_v1/lua_c/network.h to pilight/libs/pilight/lua_c/

⦁	copy or move mqttgetmsg.lua and mqttpublish.lua from mqtt_v1/actions/ to pilight/libs/pilight/events/actions/

Now pilight can be recompiled (using setup.sh in the pilight folder) and if that succeeds, the mqtt actions are available to be used in your rules. For instance:

                "mqttpublish": {
                        "rule": "IF myswitch.state == on OR myswitch.state == off  THEN mqttpublish BROKER 'localhost' TOPIC pilight/switch/myswitch MESSAGE myswitch.state QOS 1",
                        "active": 1
                },
                "mqttgetmsg": {
                        "rule": "IF (dt.second % 2) == 0  THEN mqttgetmsg BROKER 'localhost' TOPIC 'pilight/switch/#' QOS 1",
                        "active": 1
                }
The second rule checks for new mqtt messages every two seconds and returns every message published for  the specified token. If the topic of the result ends with the name of an existing swith or dimmer in the config, it will try to set the state of that switch depending on the text of the message received. Ofc for pilight the message then must be either "on" or "off" for a switch or a dimmer.

In the example above, a locally installed mosquitto broker is being used, but this can in fact be any other standard mqtt broker. In that case replace "localhost" with the hostname or ip address of the broker.
