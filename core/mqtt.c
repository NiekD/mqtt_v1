/*
	Copyright (C) 2018 CurlyMo & Niek

 This Source Code Form is subject to the terms of the Mozilla Public
 License, v. 2.0. If a copy of the MPL was not distributed with this
 file, You can obtain one at http://mozilla.org/MPL/2.0/.
*/

#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <fcntl.h>
#include <limits.h>
#include <errno.h>
#include <time.h>
#include <math.h>
#include <string.h>
#include <assert.h>
#include <sys/types.h>
#ifdef _WIN32
	#if _WIN32_WINNT < 0x0501
		#undef _WIN32_WINNT
		#define _WIN32_WINNT 0x0501
	#endif
	#define WIN32_LEAN_AND_MEAN
	#include <winsock2.h>
	#include <ws2tcpip.h>
	#define MSG_NOSIGNAL 0
#else
	#include <sys/socket.h>
	#include <sys/time.h>
	#include <netinet/in.h>
	#include <netinet/tcp.h>
	#include <netdb.h>
	#include <poll.h>
	#include <arpa/inet.h>
	#include <unistd.h>
#endif
#include <mbedtls/ssl.h>
#include <mbedtls/error.h>

#include "../../libuv/uv.h"
#include "pilight.h"
#include "socket.h"
#include "log.h"
#include "network.h"
#include "webserver.h"
#include "ssl.h"
#include "mqtt.h"


#define mqtt_publish			1
#define mqtt_getmessage			0
#define MQTT_PUBLISH 0
#define MQTT_SUBSCRIBE 1

#define PROTOCOL_NAME "MQTT" 
#define PROTOCOL_LEVEL 4	// version 3.1.1
#define CLEAN_SESSION (1U<<1)
#define KEEPALIVE 30		// specified in seconds
#define MESSAGE_ID 1		// not used by QoS 0 - value must be > 0

#define UNSUPPORTED	0
#define AUTHPLAIN	1
#define STARTTLS	2

//MQTT processing step
#define MQTT_STEP_SEND_CONNECT		1
#define MQTT_STEP_RECV_CONNACK		2
#define MQTT_STEP_SEND_PUBLISH		3
#define MQTT_STEP_RECV_PUBACK		4
#define MQTT_STEP_SEND_SUBSCRIBE	8
#define MQTT_STEP_RECV_SUBACK		9
#define MQTT_STEP_SEND_UNSUBSCRIBE	10
#define MQTT_STEP_RECV_UNSUBACK		11
#define MQTT_STEP_SEND_PINGREQ		12
#define MQTT_STEP_RECV_PINGRESP		13
#define MQTT_STEP_SEND_DISCONNECT	14
#define MQTT_STEP_SEND_PUBACK		96
#define MQTT_STEP_RECV_MESSAGE		97
#define MQTT_STEP_REMV_MESSAGE		98
#define MQTT_STEP_END				99


/*
Connection results:

Connection_Accepted 0
Connection_Refused_unacceptable_protocol_version 1
Connection_Refused_identifier_rejected 2
Connection_Refused_server_unavailable 3
Connection_Refused_bad_username_or_password 4
Connection_Refused_not_authorized 5
*/
#define Connection_Accepted 0 

typedef struct mqtt_clients_t {
	uv_poll_t *req;
	int fd;
	struct uv_custom_poll_t *data;
	struct mqtt_clients_t *next;
} mqtt_clients_t;

#ifdef _WIN32
	static uv_mutex_t mqtt_lock;
#else
	static pthread_mutex_t mqtt_lock;
	static pthread_mutexattr_t mqtt_attr;
#endif

struct mqtt_clients_t *mqtt_clients = NULL;
static int mqtt_lock_init = 0;

typedef struct request_t {
	int reqtype;
	int fd;
	char *host;
	unsigned short port;
	int is_ssl;
	char *clientid;
	char *user;
	char *pass;
	//void *userdata;
	char *packet;
	int packet_len;
	int packetid;
	int step;
	int connected;
	int authtype;
	int reading;
	int sending;
	uv_timer_t *timer_req;
	uv_poll_t *poll_req;
	size_t bytes_read;
	int status;
	int called;
	void (*callback)(int, struct mqtt_t *);

	struct mqtt_t *mqtt;

	mbedtls_ssl_context ssl;
} request_t;

static void timeout(uv_timer_t *req);
static void mqtt_client_close(uv_poll_t *req);

static unsigned char lsb(unsigned short val) {
	return (unsigned char)(val & 0x00ff);
}
static unsigned char msb(unsigned short val) {
	return (unsigned char)((val & 0xff00) >> 8) ;
}

static unsigned char get_packet_type(unsigned short val) {
	return (unsigned char)val >> 4;
}

static unsigned char set_packet_type(unsigned short val) {
	return (unsigned char)val << 4;
}

static void free_request(struct request_t *request) {
	if (request != NULL) {
		if(request->packet != NULL) {
			FREE(request->packet);
		}
		if(request->host != NULL) {
			FREE(request->host);
		}
		if(request->user != NULL) {
			FREE(request->user);
		}
		if(request->pass != NULL) {
			FREE(request->pass);
		}
		if(request->clientid != NULL) {
			FREE(request->clientid);
		}
		FREE(request);
	}
}

int mqtt_gc(void) {
	struct mqtt_clients_t *node = NULL;

#ifdef _WIN32
	uv_mutex_lock(&mqtt_lock);
#else
	pthread_mutex_lock(&mqtt_lock);
#endif
	while(mqtt_clients) {
		node = mqtt_clients;

		if(mqtt_clients->fd > -1) {
#ifdef _WIN32
			shutdown(mqtt_clients->fd, SD_BOTH);
			closesocket(mqtt_clients->fd);
#else
			shutdown(mqtt_clients->fd, SHUT_RDWR);
			close(mqtt_clients->fd);
#endif
		}

		struct uv_custom_poll_t *custom_poll_data = mqtt_clients->data;
		struct request_t *request = custom_poll_data->data;
		if(request != NULL) {
			free_request(request);
		}

		if(custom_poll_data != NULL) {
			uv_custom_poll_free(custom_poll_data);
		}

		mqtt_clients = mqtt_clients->next;
		FREE(node);
	}

#ifdef _WIN32
	uv_mutex_unlock(&mqtt_lock);
#else
	pthread_mutex_unlock(&mqtt_lock);
#endif

	logprintf(LOG_DEBUG, "MQTT garbage collected mqtt library");
	return 1;
}

static void mqtt_client_add(uv_poll_t *req, struct uv_custom_poll_t *data) {
#ifdef _WIN32
	uv_mutex_lock(&mqtt_lock);
#else
	pthread_mutex_lock(&mqtt_lock);
#endif

	int fd = -1, r = 0;

	if((r = uv_fileno((uv_handle_t *)req, (uv_os_fd_t *)&fd)) != 0) {
		logprintf(LOG_ERR, "uv_fileno: %s", uv_strerror(r));/*LCOV_EXCL_LINE*/
	}

	struct mqtt_clients_t *node = MALLOC(sizeof(struct mqtt_clients_t));
	if(node == NULL) {
		OUT_OF_MEMORY /*LCOV_EXCL_LINE*/
	}
	node->req = req;
	node->data = data;
	node->fd = fd;

	node->next = mqtt_clients;
	mqtt_clients = node;
#ifdef _WIN32
	uv_mutex_unlock(&mqtt_lock);
#else
	pthread_mutex_unlock(&mqtt_lock);
#endif
}

static void mqtt_client_remove(uv_poll_t *req) {
#ifdef _WIN32
	uv_mutex_lock(&mqtt_lock);
#else
	pthread_mutex_lock(&mqtt_lock);
#endif
	struct mqtt_clients_t *currP, *prevP;

	prevP = NULL;

	for(currP = mqtt_clients;currP != NULL;prevP = currP, currP = currP->next) {
		if(currP->req == req) {
			if(prevP == NULL) {
				mqtt_clients = currP->next;
			} else {
				prevP->next = currP->next;
			}

			FREE(currP);
			break;
		}
	}
#ifdef _WIN32
	uv_mutex_unlock(&mqtt_lock);
#else
	pthread_mutex_unlock(&mqtt_lock);
#endif
}



static void close_cb(uv_handle_t *handle) {
	/*
	 * Make sure we execute in the main thread
	 */
	const uv_thread_t pth_cur_id = uv_thread_self();
	assert(uv_thread_equal(&pth_main_id, &pth_cur_id));
	//logprintf(LOG_DEBUG, "MQTT FREE HANDLE");

	FREE(handle);
}

static void mqtt_client_close(uv_poll_t *req) {
	/*
	 * Make sure we execute in the main thread
	 */
	//logprintf(LOG_INFO, "mqtt_client_close");
	const uv_thread_t pth_cur_id = uv_thread_self();
	assert(uv_thread_equal(&pth_main_id, &pth_cur_id));

	struct uv_custom_poll_t *custom_poll_data = req->data;
	struct request_t *request = custom_poll_data->data;
	if (request != NULL && request->timer_req != NULL) {
		uv_timer_stop(request->timer_req);
	}
	if (request->mqtt->connected == 1) {
		unsigned char fixed_header[2];
		//logprintf(LOG_DEBUG, "MQTT SEND DISCONNECT");
		fixed_header[0] = set_packet_type(DISCONNECT);
		fixed_header[1] = 0;
		int packet_len = sizeof(fixed_header);
		if((request->packet = REALLOC(request->packet, packet_len)) == NULL) {
			OUT_OF_MEMORY /*LCOV_EXCL_LINE*/
		}

		memset(request->packet, 0, packet_len);
		memcpy(request->packet, &fixed_header, 2);
		
		// send Disconnect packet
		request->packet_len = packet_len;
		uv_custom_write(req);
		request->mqtt->connected = 0;
	}

	if(request->reading == 1 || 1 ==1) {
		if(request->status == 0) {
			if(request->callback != NULL && request->called == 0) {
				request->called = 1;
				request->callback(0, request->mqtt);
			}
		} else {
		
			// Callback when we were receiving data
			// that was disrupted early.
		
			if(request->callback != NULL && request->called == 0) {
				request->called = 1;
				request->callback(-1, request->mqtt);
			}
		}
	} else if(request->status == -1) {
		if(request->callback != NULL && request->called == 0) {
			request->called = 1;
			request->callback(-1, request->mqtt);
		}
	}

	if(request->mqtt->fd > -1) {
#ifdef _WIN32
		shutdown(request->mqtt->fd, SD_BOTH);
		closesocket(request->mqtt->fd);
#else
		shutdown(request->mqtt->fd, SHUT_RDWR);
		close(request->mqtt->fd);
#endif
	}

	mqtt_client_remove(req);

	if(!uv_is_closing((uv_handle_t *)req)) {
		uv_poll_stop(req);
		uv_close((uv_handle_t *)req, close_cb);
	}

	if(request != NULL) {
		free_request(request);
		custom_poll_data->data = NULL;
	}

	if(custom_poll_data != NULL) {
		uv_custom_poll_free(custom_poll_data);
		req->data = NULL;
	}
}

static void poll_close_cb(uv_poll_t *req) {
	
	//logprintf(LOG_INFO, "poll_close");

	struct uv_custom_poll_t *custom_poll_data = req->data;
	struct request_t *request = custom_poll_data->data;

	if(request->timer_req != NULL) {
		uv_timer_stop(request->timer_req);
	}

	// close client
	request->mqtt->connected = 0;
	mqtt_client_close(req);
}

static void timeout(uv_timer_t *req) {
	struct request_t *request = req->data;
	//void (*callback)(int, struct mqtt_t *) = request->callback;
	//logprintf(LOG_DEBUG, "MQTT TIMEOUT for id %i", (int)round(request->mqtt->id));
	if(request->timer_req != NULL) {
		uv_timer_stop(request->timer_req);
	}
	
// close connection
	request->mqtt->connected = 0;
	mqtt_client_close(request->poll_req);
}


static void push_data(uv_poll_t *req, int step) {
	/*
	 * Make sure we execute in the main thread
	 */
	const uv_thread_t pth_cur_id = uv_thread_self();
	assert(uv_thread_equal(&pth_main_id, &pth_cur_id));

	struct uv_custom_poll_t *custom_poll_data = req->data;
	struct request_t *request = custom_poll_data->data;

	iobuf_append(&custom_poll_data->send_iobuf, (void *)request->packet, request->packet_len);

	if(request->packet != NULL) {
		FREE(request->packet);
	}
	request->packet_len = 0;
	request->step = step;
	request->reading = 1;

	uv_custom_write(req);
	uv_custom_read(req);
}

static void read_cb(uv_poll_t *req, ssize_t *nread, char *buf) {
	/*
	 * Make sure we execute in the main thread
	 */
	const uv_thread_t pth_cur_id = uv_thread_self();
	assert(uv_thread_equal(&pth_main_id, &pth_cur_id));

	struct uv_custom_poll_t *custom_poll_data = req->data;
	struct request_t *request = custom_poll_data->data;

	//logprintf(LOG_DEBUG, "MQTT request->step=%i, packettype=%i", request->step, get_packet_type(buf[0]));

	//logprintf(LOG_INFO, "read_cb step=%i for id %i", request->step, (int)round(request->mqtt->id));

	switch(request->step) {
		case MQTT_STEP_RECV_PINGRESP: {
			//logprintf(LOG_DEBUG, "MQTT STEP RECV PINGRESP: message=%i, size=%i", get_packet_type(buf[0]), buf[1]);

			if (get_packet_type(buf[0]) == PINGRESP) {
				//logprintf(LOG_DEBUG, "MQTT received valid pingresp");
				request->status = 0;
				request->step = MQTT_STEP_SEND_CONNECT;
			}
			else {
				logprintf(LOG_NOTICE, "MQTT invalid response to ping request");
				request->status = -1;
				request->step = MQTT_STEP_SEND_CONNECT;
			}
			*nread = 0;
			uv_custom_write(req);
		} break;
		
		case MQTT_STEP_RECV_CONNACK: {
			//logprintf(LOG_DEBUG, "MQTT STEP CONNACK: message=%i, size=%i", get_packet_type(buf[0]), buf[1]);
			if( (get_packet_type(buf[0]) == CONNACK) && ((sizeof(buf) -2) == buf[1]) && (buf[3] == Connection_Accepted) ) {
				//we are connected to the broker

				//logprintf(LOG_DEBUG, "MQTT Connected to MQTT Server at %s:%4d for id %i", request->host, request->port, (int)round(request->mqtt->id));
				request->mqtt->connected = 1;
				request->status = 0;
				*nread = 0;
				
				switch (request->mqtt->reqtype) {
					case CONNECT: {
						request->step = MQTT_STEP_END;						
						uv_custom_write(req);
					} break;
					case PUBLISH: {
						request->step = MQTT_STEP_SEND_PUBLISH;	
						uv_custom_write(req);						
					} break;
					case SUBSCRIBE:
					case GETMESSAGE: {
						request->step = MQTT_STEP_SEND_SUBSCRIBE;
						uv_custom_write(req);
					} break;					
					default: {
						logprintf(LOG_NOTICE, "MQTT Invalid message type %i, %i or %i required", request->mqtt->reqtype, PUBLISH, SUBSCRIBE);
						request->status = -1;
						request->step = MQTT_STEP_END;					
						uv_custom_write(req);
					}			
				}
		
			} else {
				logprintf(LOG_NOTICE, "failed to connect to MQTT broker with error: %d", buf[3]);
				request->status = -1;
				request->step = MQTT_STEP_END;
			}	
		} break;
		
		case MQTT_STEP_RECV_PUBACK: {
			//logprintf(LOG_DEBUG, "MQTT STEP RECV PUBACK: message=%i, size=%i", get_packet_type(buf[0]), buf[1]);

			if((get_packet_type(buf[0]) == PUBACK) && (buf[1]) == 2 && (buf[2] == msb(request->mqtt->packetid) && (buf[3] == lsb(request->mqtt->packetid))) ) {
				//logprintf(LOG_DEBUG, "MQTT Published successfully to MQTT Service with QoS%i", request->mqtt->qos);
				request->status = 0;
				request->step = MQTT_STEP_END;
				*nread = 0;
			}
			else {
				//logprintf(LOG_NOTICE, "Publish to MQTT Service with QoS%i failed", request->mqtt->qos);
				request->status = -1;
				request->step = MQTT_STEP_END;
			}
			uv_custom_write(req);
		} break;

	
		case MQTT_STEP_RECV_SUBACK: {
			if((get_packet_type(buf[0]) == SUBACK) && (buf[1]) == 3 &&(buf[2] == msb(MESSAGE_ID)) && (buf[3] == lsb(MESSAGE_ID)) ) {
				//logprintf(LOG_DEBUG, "MQTT Subscribed successfully to MQTT Service with QoS%i, for topic %s", request->mqtt->qos, request->mqtt->topic);
				request->status = 0;
				*nread = 0;
				switch (request->mqtt->reqtype) {
					case GETMESSAGE: {
						if(request->timer_req != NULL) {
							uv_timer_stop(request->timer_req);
						}
						uv_timer_start(request->timer_req, (void (*)(uv_timer_t *))timeout, 100, 0);
						request->step = MQTT_STEP_RECV_MESSAGE;
						uv_custom_read(req);		
					} break;					
					default: {
						request->step = MQTT_STEP_END;
						//uv_custom_write(req);	
					}
				}
			}
			else {
				logprintf(LOG_NOTICE, "Subscribing to MQTT Service with QoS%i failed", request->mqtt->qos);
				request->status = -1;
			}
		} break;
			
		case MQTT_STEP_RECV_MESSAGE: {
			//logprintf(LOG_DEBUG, "MQTT STEP RECV MESSAGE");
//			for (int i=0;i < *nread;i++){
//				logprintf(LOG_DEBUG, "MQTT %i=>%i (%c)", i, buf[i], buf[i]);
//			}
			if( get_packet_type(buf[0]) == PUBLISH) {
				if(request->timer_req != NULL) {
					uv_timer_stop(request->timer_req);
				}
				
				unsigned int topiclen = (buf[2]<<8) + buf[3];
				if (topiclen > strlen(request->mqtt->topic)) {
					if((request->mqtt->topic = REALLOC(request->mqtt->topic, topiclen + 1)) == NULL) {
						OUT_OF_MEMORY /*LCOV_EXCL_LINE*/
					}				
				}
				memset(request->mqtt->topic, 0, topiclen + 1);
				memcpy(request->mqtt->topic, buf + 4, topiclen);
				
				unsigned long msgpos = 4 + topiclen;
				
				if (((buf[0]>>1) & 0x03) > 0) {//if qos is 1 or 2, get the packet id
					unsigned short packetid = (buf[4 + topiclen] << 8) + buf[4 + topiclen+1];
					//logprintf(LOG_DEBUG, "MQTT Packet ID is %d", packetid);
					request->packetid = packetid;
					msgpos += 2;
				}

				unsigned int msglen = buf[1] + 2 - msgpos;
				
				if(msglen > strlen(request->mqtt->message)) {
					if((request->mqtt->message = REALLOC(request->mqtt->message, msglen + 1)) == NULL) {
						OUT_OF_MEMORY /*LCOV_EXCL_LINE*/
					}				
				}
				
				memset(request->mqtt->message, 0, msglen + 1);
				memcpy(request->mqtt->message, buf + msgpos, msglen);

				//logprintf(LOG_DEBUG, "MQTT received topic \"%s\", message \"%s\"", request->mqtt->topic, request->mqtt->message);
				
				*nread = 0;
				request->step = MQTT_STEP_SEND_PUBACK;
				uv_custom_write(req);
			}
		} break;
	}
}

static void write_cb(uv_poll_t *req) {
	/*
	 * Make sure we execute in the main thread
	 */
	const uv_thread_t pth_cur_id = uv_thread_self();
	assert(uv_thread_equal(&pth_main_id, &pth_cur_id));

	struct uv_custom_poll_t *custom_poll_data = req->data;
	struct request_t *request = custom_poll_data->data;
	unsigned char fixed_header[2];

	//logprintf(LOG_INFO, "write_cb step=%i for id %i", request->step, (int)round(request->mqtt->id));

	switch(request->step) {
		case MQTT_STEP_SEND_PINGREQ: { 
			fixed_header[0] = set_packet_type(PING);
			fixed_header[1] = 0;
			if((request->packet = REALLOC(request->packet, 2)) == NULL) {
					OUT_OF_MEMORY /*LCOV_EXCL_LINE*/
			}
			memset(request->packet, 0, 2);
			memcpy(request->packet, fixed_header, sizeof(fixed_header));
			request->packet_len = 2;
			push_data(req, MQTT_STEP_RECV_PINGRESP);
		} break;
		
		case MQTT_STEP_SEND_CONNECT: {
			unsigned char var_header[10];
			var_header[0] = 0;
			var_header[1] = strlen(PROTOCOL_NAME);
			memcpy(&var_header[2], PROTOCOL_NAME, strlen(PROTOCOL_NAME));
			var_header[6] = PROTOCOL_LEVEL;
			var_header[7] = CLEAN_SESSION;
			var_header[8] = msb(KEEPALIVE);
			var_header[9] = lsb(KEEPALIVE);

			unsigned char payload[strlen(request->mqtt->clientid) + 2];
			payload[0] = 0;
			payload[1] = strlen(request->mqtt->clientid);
			memcpy(&payload[2],request->mqtt->clientid,strlen(request->mqtt->clientid));

			fixed_header[0] = set_packet_type(CONNECT);
			fixed_header[1] = sizeof(var_header) + sizeof(payload);

			int packet_len = 2 + sizeof(var_header) + sizeof(payload);
			if((request->packet = REALLOC(request->packet, packet_len)) == NULL) {
				OUT_OF_MEMORY /*LCOV_EXCL_LINE*/
			}

			memset(request->packet, 0, packet_len);
			memcpy(request->packet, fixed_header, sizeof(fixed_header));
			memcpy(request->packet + sizeof(fixed_header),var_header,sizeof(var_header));
			memcpy(request->packet + sizeof(fixed_header)+sizeof(var_header),payload,sizeof(payload));
			
			// send Connect message
			request->packet_len = packet_len;
			push_data(req, MQTT_STEP_RECV_CONNACK);
		} break;
		
		case MQTT_STEP_SEND_PUBLISH: {
			//logprintf(LOG_DEBUG, "MQTT SEND PUBLISH Qos%i", request->mqtt->qos);
			if(request->mqtt->qos == 0) {
				unsigned char topic[strlen(request->mqtt->topic) + 2];
				topic[0] = 0;
				topic[1] = lsb((unsigned short)strlen(request->mqtt->topic));
				memcpy((char *)&topic[2], request->mqtt->topic, strlen(request->mqtt->topic));
					
				fixed_header[0] = set_packet_type(PUBLISH)| 1;//retained
				fixed_header[1] = sizeof(topic) + strlen(request->mqtt->message);

				int packet_len = 2 + sizeof(topic) + strlen(request->mqtt->message);
				if((request->packet = REALLOC(request->packet, packet_len)) == NULL) {
					OUT_OF_MEMORY /*LCOV_EXCL_LINE*/
				}

				memset(request->packet, 0, packet_len);
				memcpy(request->packet, fixed_header, sizeof(fixed_header));
				memcpy(request->packet + sizeof(fixed_header), topic, sizeof(topic));
				memcpy(request->packet + sizeof(fixed_header) + sizeof(topic), request->mqtt->message, strlen(request->mqtt->message));
				request->packet_len = packet_len;

				push_data(req, MQTT_STEP_END);
			} else {
				unsigned char topic[strlen(request->mqtt->topic) + 4];// 2 extra for message size > QoS0 for msg ID
				topic[0] = 0;
				topic[1] = lsb(strlen(request->mqtt->topic));
				memcpy((char *)&topic[2], request->mqtt->topic, strlen(request->mqtt->topic));
				
				//generate unique packetid 
				request->mqtt->packetid = 123;//just for testing, 
				topic[sizeof(topic) -2] = msb(request->mqtt->packetid);
				topic[sizeof(topic) -1] = lsb(request->mqtt->packetid);
				
				fixed_header[0] = set_packet_type(PUBLISH)|(request->mqtt->qos << 1)| 1;//retained
				fixed_header[1] = sizeof(topic) + strlen(request->mqtt->message);
				
				int packet_len = 2 + sizeof(topic) + strlen(request->mqtt->message);
				
				if((request->packet = REALLOC(request->packet, packet_len)) == NULL) {
					OUT_OF_MEMORY /*LCOV_EXCL_LINE*/
				}
				
				memset(request->packet, 0, packet_len);
				memcpy(request->packet, fixed_header, sizeof(fixed_header));
				memcpy(request->packet + sizeof(fixed_header), topic, sizeof(topic));
				memcpy(request->packet + sizeof(fixed_header) + sizeof(topic), request->mqtt->message, strlen(request->mqtt->message));
				request->packet_len = packet_len;

				push_data(req, MQTT_STEP_RECV_PUBACK);				
			}

		} break;
		
		case MQTT_STEP_SEND_SUBSCRIBE: {
			//logprintf(LOG_DEBUG, "MQTT SEND SUBSCRIBE Qos%i", request->mqtt->qos);
			unsigned char var_header[2];
			var_header[0] = msb(MESSAGE_ID);
			var_header[1] = lsb(MESSAGE_ID);
			unsigned char topic[strlen(request->mqtt->topic) + 3];
			topic[0] = 0;
			topic[1] = lsb(strlen(request->mqtt->topic));
			memcpy((char *)&topic[2], request->mqtt->topic, strlen(request->mqtt->topic));
			topic[sizeof(topic)-1] = request->mqtt->qos;	
			fixed_header[0] = set_packet_type(SUBSCRIBE) + 2;
			fixed_header[1] = 2 + sizeof(topic);
			int packet_len = 4 + sizeof(topic);
			if((request->packet = REALLOC(request->packet, packet_len)) == NULL) {
				OUT_OF_MEMORY /*LCOV_EXCL_LINE*/
			}

			memset(request->packet, 0, packet_len);
		
			memcpy(request->packet, fixed_header, 2);
			memcpy(request->packet + 2, var_header, 2);
			memcpy(request->packet + 4, topic, sizeof(topic));
			request->packet_len = packet_len;

			push_data(req, MQTT_STEP_RECV_SUBACK);

		} break;
		
		case MQTT_STEP_SEND_PUBACK: {
			//logprintf(LOG_DEBUG, "MQTT SEND PUBACK");
			unsigned char var_header[2];
			var_header[0] = 0;
			var_header[1] = 1;//packet id from received message TO BE DONE
			
			fixed_header[0] = set_packet_type(PUBACK);
			fixed_header[1] = sizeof(var_header);
			
			int packet_len = sizeof(fixed_header) + sizeof(var_header);
			if((request->packet = REALLOC(request->packet, packet_len)) == NULL) {
				OUT_OF_MEMORY /*LCOV_EXCL_LINE*/
			}

			memset(request->packet, 0, packet_len);
		
			memcpy(request->packet, fixed_header, 2);
			memcpy(request->packet + 2, var_header, 2);
			request->packet_len = packet_len;

			push_data(req, MQTT_STEP_REMV_MESSAGE);
		} break;
		
		case MQTT_STEP_REMV_MESSAGE: {// publish empty message to delete retained message from broker
			//logprintf(LOG_DEBUG, "MQTT REMOVE MESSAGE Qos%i, Topic %s", request->mqtt->qos, request->mqtt->topic);
			if(request->mqtt->qos == 0) {
				unsigned char topic[strlen(request->mqtt->topic) + 2];
				topic[0] = 0;
				topic[1] = lsb((unsigned short)strlen(request->mqtt->topic));
				memcpy((char *)&topic[2], request->mqtt->topic, strlen(request->mqtt->topic));
					
				fixed_header[0] = set_packet_type(PUBLISH)| 1;//retained
				fixed_header[1] = sizeof(topic);

				int packet_len = 2 + sizeof(topic);
				if((request->packet = REALLOC(request->packet, packet_len)) == NULL) {
					OUT_OF_MEMORY /*LCOV_EXCL_LINE*/
				}

				memset(request->packet, 0, packet_len);
				memcpy(request->packet, fixed_header, sizeof(fixed_header));
				memcpy(request->packet + sizeof(fixed_header), topic, sizeof(topic));
				request->packet_len = packet_len;

				push_data(req, MQTT_STEP_END);
			} else {
				unsigned char topic[strlen(request->mqtt->topic) + 4];// 2 extra for message size > QoS0 for msg ID
				topic[0] = 0;
				topic[1] = lsb(strlen(request->mqtt->topic));
				memcpy((char *)&topic[2], request->mqtt->topic, strlen(request->mqtt->topic));
				
				//generate unique packetid 
				request->mqtt->packetid = 123;//just for testing, 
				topic[sizeof(topic) -2] = msb(request->mqtt->packetid);
				topic[sizeof(topic) -1] = lsb(request->mqtt->packetid);
				
				fixed_header[0] = set_packet_type(PUBLISH)|(request->mqtt->qos << 1)| 1;//retained
				fixed_header[1] = sizeof(topic);
				
				int packet_len = 2 + sizeof(topic);
				
				if((request->packet = REALLOC(request->packet, packet_len)) == NULL) {
					OUT_OF_MEMORY /*LCOV_EXCL_LINE*/
				}
				
				memset(request->packet, 0, packet_len);
				memcpy(request->packet, fixed_header, sizeof(fixed_header));
				memcpy(request->packet + sizeof(fixed_header), topic, sizeof(topic));
				request->packet_len = packet_len;
				push_data(req, MQTT_STEP_RECV_PUBACK);				
			}

		} break;		
		case MQTT_STEP_SEND_DISCONNECT: {
			
			//logprintf(LOG_DEBUG, "MQTT SEND DISCONNECT");
			fixed_header[0] = set_packet_type(DISCONNECT);
			fixed_header[1] = 0;
			int packet_len = sizeof(fixed_header);
			if((request->packet = REALLOC(request->packet, packet_len)) == NULL) {
				OUT_OF_MEMORY /*LCOV_EXCL_LINE*/
			}

			memset(request->packet, 0, packet_len);
			memcpy(request->packet, &fixed_header, 2);
			
			// send Disconnect message
			request->packet_len = packet_len;	
			push_data(req, MQTT_STEP_END);
			request->mqtt->connected = 0;
			
		} break;
		
		case MQTT_STEP_END: {
			//logprintf(LOG_DEBUG, "MQTT END");
			uv_custom_close(req);
			return;
		}
	}
}

int mqtt_request(char *host, char *user, char *pass, unsigned short port, int is_ssl, struct mqtt_t *mqtt, void (*callback)(int, struct mqtt_t *)) {
	logprintf(LOG_DEBUG, "MQTT broker called with host: %s, port: %i, clientid: %s", host, port, mqtt->clientid);
	struct request_t *request = NULL;
	struct uv_custom_poll_t *custom_poll_data = NULL;
	struct sockaddr_in addr4;
	struct sockaddr_in6 addr6;
	char *ip = NULL;
	int r = 0;
	
	request = MALLOC (sizeof(request_t));
	if(!request) {
		logprintf(LOG_ERR, "out of memory");
		exit(EXIT_FAILURE);
	}

	if(mqtt->topic == NULL) {
		logprintf(LOG_ERR, "MQTT:topic not set");
		return -1;
	}
	if(mqtt->message == NULL && request->mqtt->reqtype == PUBLISH) {
		logprintf(LOG_ERR, "MQTT: message not set");
		return -1;
	}

	if((request = MALLOC(sizeof(struct request_t))) == NULL) {
		OUT_OF_MEMORY /*LCOV_EXCL_LINE*/
	}
	memset(request, 0, sizeof(struct request_t));

	request->host = MALLOC(strlen(host)+1);
	if(!request->host) {
		logprintf(LOG_ERR, "out of memory");
		exit(EXIT_FAILURE);
	}
	strcpy(request->host, host);

	request->user = MALLOC(strlen(user)+1);
	if(!request->user) {
		logprintf(LOG_ERR, "out of memory");
		exit(EXIT_FAILURE);
	}
	strcpy(request->user, user);

	request->pass = MALLOC(strlen(pass)+1);
	if(!request->pass) {
		logprintf(LOG_ERR, "out of memory");
		exit(EXIT_FAILURE);
	}
	strcpy(request->pass, pass);

	request->port = port;
	request->status = -1; //if request succeeds will be set to 0
	request->mqtt = mqtt;
	request->authtype = UNSUPPORTED;

	request->callback = callback;
	request->mqtt->connected = 0;
		
		if(mqtt_lock_init == 0) {
			mqtt_lock_init = 1;
	#ifdef _WIN32
			uv_mutex_init(&mqtt_lock);
	#else
			pthread_mutexattr_init(&mqtt_attr);
			pthread_mutexattr_settype(&mqtt_attr, PTHREAD_MUTEX_RECURSIVE);
			pthread_mutex_init(&mqtt_lock, &mqtt_attr);
	#endif
		}

	#ifdef _WIN32
		WSADATA wsa;

		if(WSAStartup(0x202, &wsa) != 0) {
			logprintf(LOG_ERR, "WSAStartup");
			exit(EXIT_FAILURE);
		}
	#endif

		memset(&addr4, 0, sizeof(addr4));
		memset(&addr6, 0, sizeof(addr6));
		int inet = host2ip(request->host, &ip);
		switch(inet) {
			case AF_INET: {
				memset(&addr4, '\0', sizeof(struct sockaddr_in));
				r = uv_ip4_addr(ip, request->port, &addr4);
				if(r != 0) {
					/*LCOV_EXCL_START*/
					logprintf(LOG_ERR, "uv_ip4_addr: %s", uv_strerror(r));
					goto freeuv;
					/*LCOV_EXCL_END*/
				}
			} break;
			case AF_INET6: {
				memset(&addr6, '\0', sizeof(struct sockaddr_in6));
				r = uv_ip6_addr(ip, request->port, &addr6);
				if(r != 0) {
					/*LCOV_EXCL_START*/
					logprintf(LOG_ERR, "uv_ip6_addr: %s", uv_strerror(r));
					goto freeuv;
					/*LCOV_EXCL_END*/
				}
			} break;
			default: {
				/*LCOV_EXCL_START*/
				logprintf(LOG_ERR, "host2ip");
				goto freeuv;
				/*LCOV_EXCL_END*/
			} break;
		}
		FREE(ip);

		/*
		 * Partly bypass libuv in case of ssl connections
		 */
		if((request->mqtt->fd = socket(inet, SOCK_STREAM, 0)) < 0){
			/*LCOV_EXCL_START*/
			logprintf(LOG_ERR, "socket: %s", strerror(errno));
			goto freeuv;
			/*LCOV_EXCL_STOP*/
		}

	#ifdef _WIN32
		unsigned long on = 1;
		ioctlsocket(request->mqtt->fd, FIONBIO, &on);
	#else
		long arg = fcntl(request->mqtt->fd, F_GETFL, NULL);
		fcntl(request->mqtt->fd, F_SETFL, arg | O_NONBLOCK);
	#endif

		switch(inet) {
			case AF_INET: {
				r = connect(request->mqtt->fd, (struct sockaddr *)&addr4, sizeof(addr4));
			} break;
			case AF_INET6: {
				r = connect(request->mqtt->fd, (struct sockaddr *)&addr6, sizeof(addr6));
			} break;
			default: {
			} break;
		}

		if(r < 0) {
	#ifdef _WIN32
			if(!(WSAGetLastError() == WSAEWOULDBLOCK || WSAGetLastError() == WSAEISCONN)) {
	#else
			if(!(errno == EINPROGRESS || errno == EISCONN)) {
	#endif
				/*LCOV_EXCL_START*/
				logprintf(LOG_ERR, "connect: %s", strerror(errno));
				goto freeuv;
				/*LCOV_EXCL_STOP*/
			}
		}

	//logprintf(LOG_DEBUG, "Network connection established fd=%i", request->mqtt->fd);

	
	request->poll_req = NULL;
	if((request->poll_req = MALLOC(sizeof(uv_poll_t))) == NULL) {
		OUT_OF_MEMORY /*LCOV_EXCL_LINE*/
	}
	if((request->timer_req = MALLOC(sizeof(uv_timer_t))) == NULL) {
		OUT_OF_MEMORY /*LCOV_EXCL_LINE*/
	}
	uv_custom_poll_init(&custom_poll_data, request->poll_req, (void *)request);
	custom_poll_data->is_ssl = request->is_ssl;
	custom_poll_data->write_cb = write_cb;
	custom_poll_data->read_cb = read_cb;
	custom_poll_data->close_cb = poll_close_cb;
	if((custom_poll_data->host = STRDUP(request->host)) == NULL) {
		OUT_OF_MEMORY
	}

	request->timer_req->data = request;

	r = uv_poll_init_socket(uv_default_loop(), request->poll_req, request->mqtt->fd);
	if(r != 0) {
		/*LCOV_EXCL_START*/
		logprintf(LOG_ERR, "uv_poll_init_socket: %s", uv_strerror(r));
		FREE(request->poll_req);
		goto freeuv;
		/*LCOV_EXCL_STOP*/
	}
	
	uv_timer_init(uv_default_loop(), request->timer_req);
	uv_timer_start(request->timer_req, (void (*)(uv_timer_t *))timeout, 3000, 0); //

	mqtt_client_add(request->poll_req, custom_poll_data);
	
	//logprintf(LOG_INFO, "Request type = %i, fd = %i, connected = %i,", request->mqtt->reqtype, request->mqtt->fd, request->mqtt->connected);

	request->step = MQTT_STEP_SEND_CONNECT;
	uv_custom_write(request->poll_req);
	
	return 0;

freeuv:
	if(request->timer_req != NULL) {
		uv_timer_stop(request->timer_req);
	}
	if(request->callback != NULL && request->called == 0) {
		request->called = 1;
		request->callback(-1, request->mqtt);
	}
	//FREE(request->host);

	if(request->mqtt->fd > 0) {
#ifdef _WIN32
		closesocket(request->mqtt->fd);
#else
		close(request->mqtt->fd);
#endif
	}
	FREE(request);
	return 0;

}
