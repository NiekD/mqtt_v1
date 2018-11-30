/*
	Copyright (C) 2018 CurlyMo & Niek
  This Source Code Form is subject to the terms of the Mozilla Public
  License, v. 2.0. If a copy of the MPL was not distributed with this
  file, You can obtain one at http://mozilla.org/MPL/2.0/.
*/

#ifndef _MQTT_H_
#define _MQTT_H_

//MQTT message types:
#define CONNECT 1
#define CONNACK 2
#define PUBLISH 3
#define PUBACK 4
#define PUBREC 5
#define PUBREL 6
#define PUBCOMP 7
#define SUBSCRIBE 8
#define SUBACK 9
#define UNSUBSCRIBE 10
#define UNSUBACK 11
#define PINGREQ 12
#define PINGRESP 13
#define DISCONNECT 14
#define GETMESSAGE 99

typedef struct mqtt_t {
	double id;
	char *topic;
	char *message;
	int qos;
	int packetid;
	int reqtype;
	int fd;
	int connected;
	char *clientid;
	void *data;
} mqtt_t;


int mqtt_request(char *, char*, char *, unsigned short, int, struct mqtt_t *, void (*)(int, struct mqtt_t *));

int mqtt_gc(void);
#endif
