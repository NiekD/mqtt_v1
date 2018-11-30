/*
	Copyright (C) 2018 CurlyMo & Niek

  This Source Code Form is subject to the terms of the Mozilla Public
  License, v. 2.0. If a copy of the MPL was not distributed with this
  file, You can obtain one at http://mozilla.org/MPL/2.0/.
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <limits.h>
#include <assert.h>

#ifndef _WIN32
	#include <unistd.h>
	#include <sys/time.h>
	#include <libgen.h>
	#include <dirent.h>
	#include <unistd.h>
#endif

#include "../../core/log.h"
#include "../../core/mqtt.h"
#include "../network.h"

typedef struct lua_mqtt_t {
	struct plua_metatable_t *table;
	struct plua_module_t *module;
	lua_State *L;

	struct mqtt_t mqtt;

	char *host;
	char *user;
	char *password;
	int port;
	int is_ssl;
	char *callback;
	
	int status;
} lua_mqtt_t;

static void plua_network_mqtt_object(lua_State *L, struct lua_mqtt_t *mqtt);
static void plua_network_mqtt_gc(void *ptr);

static int plua_network_mqtt_set_data(lua_State *L) {
	struct lua_mqtt_t *mqtt = (void *)lua_topointer(L, lua_upvalueindex(1));
	struct plua_metatable_t *cpy = NULL;

	if(lua_gettop(L) != 1) {
		luaL_error(L, "mqtt.setData requires 1 argument, %d given", lua_gettop(L));
	}

	if(mqtt == NULL) {
		luaL_error(L, "internal error: mqtt object not passed");
	}

	char buf[128] = { '\0' }, *p = buf;
	char *error = "userdata expected, got %s";

	sprintf(p, error, lua_typename(L, lua_type(L, -1)));

	luaL_argcheck(L,
		(lua_type(L, -1) == LUA_TLIGHTUSERDATA || lua_type(L, -1) == LUA_TTABLE),
		1, buf);

	if(lua_type(L, -1) == LUA_TLIGHTUSERDATA) {
		cpy = (void *)lua_topointer(L, -1);
		lua_remove(L, -1);
		plua_metatable_clone(&cpy, &mqtt->table);

		plua_ret_true(L);

		return 1;
	}

	if(lua_type(L, -1) == LUA_TTABLE) {
		lua_pushnil(L);
		while(lua_next(L, -2) != 0) {
			plua_metatable_parse_set(L, mqtt->table);
			lua_pop(L, 1);
		}

		plua_ret_true(L);
		return 1;
	}

	plua_ret_false(L);

	return 0;
}

static int plua_network_mqtt_get_data(lua_State *L) {
	struct lua_mqtt_t *mqtt = (void *)lua_topointer(L, lua_upvalueindex(1));

	if(lua_gettop(L) != 0) {
		luaL_error(L, "mqtt.setUserdata requires 0 argument, %d given", lua_gettop(L));
		return 0;
	}

	if(mqtt == NULL) {
		luaL_error(L, "internal error: mqtt object not passed");
		return 0;
	}

	lua_newtable(L);
//	lua_newtable(L);

	lua_pushstring(L, "__index");
	lua_pushlightuserdata(L, mqtt->table);
	lua_pushcclosure(L, plua_metatable_get, 1);
	lua_settable(L, -3);

	lua_pushstring(L, "__newindex");
	lua_pushlightuserdata(L, mqtt->table);
	lua_pushcclosure(L, plua_metatable_set, 1);
	lua_settable(L, -3);

	lua_pushstring(L, "__gc");
	lua_pushlightuserdata(L, mqtt->table);
	lua_pushcclosure(L, plua_metatable_gc, 1);
	lua_settable(L, -3);

	lua_pushstring(L, "__pairs");
	lua_pushlightuserdata(L, mqtt->table);
	lua_pushcclosure(L, plua_metatable_pairs, 1);
	lua_settable(L, -3);

	lua_pushstring(L, "__next");
	lua_pushlightuserdata(L, mqtt->table);
	lua_pushcclosure(L, plua_metatable_next, 1);
	lua_settable(L, -3);

	lua_pushstring(L, "__call");
	lua_pushlightuserdata(L, mqtt->table);
	lua_pushcclosure(L, plua_metatable_call, 1);
	lua_settable(L, -3);

	lua_setmetatable(L, -2);

	assert(lua_gettop(L) == 1);

	return 1;
}

static int plua_network_mqtt_set_topic(lua_State *L) {
	struct lua_mqtt_t *mqtt = (void *)lua_topointer(L, lua_upvalueindex(1));
	char *topic = NULL;

	if(lua_gettop(L) != 1) {
		luaL_error(L, "mqtt.setTopic requires 1 argument, %d given", lua_gettop(L));
	}

	if(mqtt == NULL) {
		luaL_error(L, "internal error: mqtt object not passed");
	}

	char buf[128] = { '\0' }, *p = buf;
	char *error = "string expected, got %s";

	sprintf(p, error, lua_typename(L, lua_type(L, -1)));

	luaL_argcheck(L,
		(lua_type(L, -1) == LUA_TSTRING),
		1, buf);

	topic = (void *)lua_tostring(L, -1);
	if((mqtt->mqtt.topic = STRDUP(topic)) == NULL) {
		OUT_OF_MEMORY
	}
	lua_remove(L, -1);

	lua_pushboolean(L, 1);

	assert(lua_gettop(L) == 1);

	return 1;
}

static int plua_network_mqtt_set_clientid(lua_State *L) {
	struct lua_mqtt_t *mqtt = (void *)lua_topointer(L, lua_upvalueindex(1));
	char *clientid = NULL;

	if(lua_gettop(L) != 1) {
		luaL_error(L, "mqtt.setClienid requires 1 argument, %d given", lua_gettop(L));
	}

	if(mqtt == NULL) {
		luaL_error(L, "internal error: mqtt object not passed");
	}

	char buf[128] = { '\0' }, *p = buf;
	char *error = "string expected, got %s";

	sprintf(p, error, lua_typename(L, lua_type(L, -1)));

	luaL_argcheck(L,
		(lua_type(L, -1) == LUA_TSTRING),
		1, buf);

	clientid = (void *)lua_tostring(L, -1);
	if((mqtt->mqtt.clientid = STRDUP(clientid)) == NULL) {
		OUT_OF_MEMORY
	}
	lua_remove(L, -1);

	lua_pushboolean(L, 1);

	assert(lua_gettop(L) == 1);

	return 1;
}


static int plua_network_mqtt_set_message(lua_State *L) {
	struct lua_mqtt_t *mqtt = (void *)lua_topointer(L, lua_upvalueindex(1));
	char *message = NULL;

	if(lua_gettop(L) != 1) {
		luaL_error(L, "mqtt.setMessage requires 1 argument, %d given", lua_gettop(L));
	}

	if(mqtt == NULL) {
		luaL_error(L, "internal error: mqtt object not passed");
	}

	char buf[128] = { '\0' }, *p = buf;
	char *error = "string expected, got %s";

	sprintf(p, error, lua_typename(L, lua_type(L, -1)));

	luaL_argcheck(L,
		(lua_type(L, -1) == LUA_TSTRING),
		1, buf);

	message = (void *)lua_tostring(L, -1);
	if((mqtt->mqtt.message = STRDUP(message)) == NULL) {
		OUT_OF_MEMORY
	}
	lua_remove(L, -1);

	lua_pushboolean(L, 1);

	assert(lua_gettop(L) == 1);

	return 1;
}

static int plua_network_mqtt_set_port(lua_State *L) {
	struct lua_mqtt_t *mqtt = (void *)lua_topointer(L, lua_upvalueindex(1));
	int port = 0;

	if(lua_gettop(L) != 1) {
		luaL_error(L, "mqtt.setPort requires 1 argument, %d given", lua_gettop(L));
	}

	if(mqtt == NULL) {
		luaL_error(L, "internal error: mqtt object not passed");
	}

	char buf[128] = { '\0' }, *p = buf;
	char *error = "number expected, got %s";

	sprintf(p, error, lua_typename(L, lua_type(L, -1)));

	luaL_argcheck(L,
		(lua_type(L, -1) == LUA_TNUMBER),
		1, buf);

	port = lua_tonumber(L, -1);
	mqtt->port = port;

	lua_remove(L, -1);

	lua_pushboolean(L, 1);

	assert(lua_gettop(L) == 1);

	return 1;
}
static int plua_network_mqtt_set_reqtype(lua_State *L) {
	struct lua_mqtt_t *mqtt = (void *)lua_topointer(L, lua_upvalueindex(1));
	int reqtype = 0;

	if(lua_gettop(L) != 1) {
		luaL_error(L, "mqtt.setReqtype requires 1 argument, %d given", lua_gettop(L));
	}

	if(mqtt == NULL) {
		luaL_error(L, "internal error: mqtt object not passed");
	}

	char buf[128] = { '\0' }, *p = buf;
	char *error = "number expected, got %s";

	sprintf(p, error, lua_typename(L, lua_type(L, -1)));

	luaL_argcheck(L,
		(lua_type(L, -1) == LUA_TNUMBER),
		1, buf);

	reqtype = lua_tonumber(L, -1);
	mqtt->mqtt.reqtype = reqtype;

	lua_remove(L, -1);

	lua_pushboolean(L, 1);

	assert(lua_gettop(L) == 1);

	return 1;
}

static int plua_network_mqtt_set_host(lua_State *L) {
	struct lua_mqtt_t *mqtt = (void *)lua_topointer(L, lua_upvalueindex(1));
	char *host = NULL;

	if(lua_gettop(L) != 1) {
		luaL_error(L, "mqtt.setHost requires 1 argument, %d given", lua_gettop(L));
	}

	if(mqtt == NULL) {
		luaL_error(L, "internal error: mqtt object not passed");
	}

	char buf[128] = { '\0' }, *p = buf;
	char *error = "string expected, got %s";

	sprintf(p, error, lua_typename(L, lua_type(L, -1)));

	luaL_argcheck(L,
		(lua_type(L, -1) == LUA_TSTRING),
		1, buf);

	host = (void *)lua_tostring(L, -1);
	if((mqtt->host = STRDUP(host)) == NULL) {
		OUT_OF_MEMORY
	}
	lua_remove(L, -1);

	lua_pushboolean(L, 1);

	assert(lua_gettop(L) == 1);

	return 1;
}

static int plua_network_mqtt_set_user(lua_State *L) {
	struct lua_mqtt_t *mqtt = (void *)lua_topointer(L, lua_upvalueindex(1));
	char *user = NULL;

	if(lua_gettop(L) != 1) {
		luaL_error(L, "mqtt.setUser requires 1 argument, %d given", lua_gettop(L));
	}

	if(mqtt == NULL) {
		luaL_error(L, "internal error: mqtt object not passed");
	}

	char buf[128] = { '\0' }, *p = buf;
	char *error = "string expected, got %s";

	sprintf(p, error, lua_typename(L, lua_type(L, -1)));

	luaL_argcheck(L,
		(lua_type(L, -1) == LUA_TSTRING),
		1, buf);

	user = (void *)lua_tostring(L, -1);
	if((mqtt->user = STRDUP(user)) == NULL) {
		OUT_OF_MEMORY
	}
	lua_remove(L, -1);

	lua_pushboolean(L, 1);

	assert(lua_gettop(L) == 1);

	return 1;
}

static int plua_network_mqtt_set_password(lua_State *L) {
	struct lua_mqtt_t *mqtt = (void *)lua_topointer(L, lua_upvalueindex(1));
	char *password = NULL;

	if(lua_gettop(L) != 1) {
		luaL_error(L, "mqtt.setPassword requires 1 argument, %d given", lua_gettop(L));
	}

	if(mqtt == NULL) {
		luaL_error(L, "internal error: mqtt object not passed");
	}

	char buf[128] = { '\0' }, *p = buf;
	char *error = "string expected, got %s";

	sprintf(p, error, lua_typename(L, lua_type(L, -1)));

	luaL_argcheck(L,
		(lua_type(L, -1) == LUA_TSTRING),
		1, buf);

	password = (void *)lua_tostring(L, -1);
	if((mqtt->password = STRDUP(password)) == NULL) {
		OUT_OF_MEMORY
	}
	lua_remove(L, -1);

	lua_pushboolean(L, 1);

	assert(lua_gettop(L) == 1);

	return 1;
}

static int plua_network_mqtt_set_callback(lua_State *L) {
	struct lua_mqtt_t *mqtt = (void *)lua_topointer(L, lua_upvalueindex(1));
	char *func = NULL;

	if(lua_gettop(L) != 1) {
		luaL_error(L, "mqtt.setCallback requires 1 argument, %d given", lua_gettop(L));
	}

	if(mqtt == NULL) {
		luaL_error(L, "internal error: mqtt object not passed");
	}

	if(mqtt->module == NULL) {
		luaL_error(L, "internal error: lua state not properly initialized");
	}

	char buf[128] = { '\0' }, *p = buf, name[255] = { '\0' };
	char *error = "string expected, got %s";

	sprintf(p, error, lua_typename(L, lua_type(L, -1)));

	luaL_argcheck(L,
		(lua_type(L, -1) == LUA_TSTRING),
		1, buf);

	func = (void *)lua_tostring(L, -1);
	lua_remove(L, -1);

	p = name;
	switch(mqtt->module->type) {
		case FUNCTION: {
			sprintf(p, "function.%s", mqtt->module->name);
		} break;
		case OPERATOR: {
			sprintf(p, "operator.%s", mqtt->module->name);
		} break;
		case ACTION: {
			sprintf(p, "action.%s", mqtt->module->name);
		} break;
	}

	lua_getglobal(L, name);
	if(lua_type(L, -1) == LUA_TNIL) {
		luaL_error(L, "cannot find %s lua module", mqtt->module->name);
	}

	lua_getfield(L, -1, func);
	if(lua_type(L, -1) != LUA_TFUNCTION) {
		luaL_error(L, "%s: mqtt callback %s does not exist", mqtt->module->file, func);
	}
	lua_remove(L, -1);
	lua_remove(L, -1);

	if((mqtt->callback = STRDUP(func)) == NULL) {
		OUT_OF_MEMORY
	}

	lua_pushboolean(L, 1);

	assert(lua_gettop(L) == 1);

	return 1;
}

static int plua_network_mqtt_set_qos(lua_State *L) {
	struct lua_mqtt_t *mqtt = (void *)lua_topointer(L, lua_upvalueindex(1));
	int qos = 0;

	if(lua_gettop(L) != 1) {
		luaL_error(L, "mqtt.setQos requires 1 argument, %d given", lua_gettop(L));
	}

	if(mqtt == NULL) {
		luaL_error(L, "internal error: mqtt object not passed");
	}

	char buf[128] = { '\0' }, *p = buf;
	char *error = "number expected, got %s";

	sprintf(p, error, lua_typename(L, lua_type(L, -1)));

	luaL_argcheck(L,
		(lua_type(L, -1) == LUA_TNUMBER) || (lua_type(L, -1) == LUA_TBOOLEAN),
		1, buf);

	qos = lua_tonumber(L, -1);
	mqtt->mqtt.qos = qos;


	lua_remove(L, -1);

	lua_pushboolean(L, 1);

	assert(lua_gettop(L) == 1);

	return 1;
}

static int plua_network_mqtt_set_ssl(lua_State *L) {
	struct lua_mqtt_t *mqtt = (void *)lua_topointer(L, lua_upvalueindex(1));
	int ssl = 0;

	if(lua_gettop(L) != 1) {
		luaL_error(L, "mqtt.setSSL requires 1 argument, %d given", lua_gettop(L));
	}

	if(mqtt == NULL) {
		luaL_error(L, "internal error: mqtt object not passed");
	}

	char buf[128] = { '\0' }, *p = buf;
	char *error = "number expected, got %s";

	sprintf(p, error, lua_typename(L, lua_type(L, -1)));

	luaL_argcheck(L,
		(lua_type(L, -1) == LUA_TNUMBER) || (lua_type(L, -1) == LUA_TBOOLEAN),
		1, buf);

	ssl = lua_tonumber(L, -1);
	mqtt->is_ssl = ssl;


	lua_remove(L, -1);

	lua_pushboolean(L, 1);

	assert(lua_gettop(L) == 1);

	return 1;
}

static void plua_network_mqtt_callback(int status, struct mqtt_t *mqtt) {
	struct lua_mqtt_t *data = mqtt->data;
	char name[255], *p = name;
	memset(name, '\0', 255);

	/*
	 * Only create a new state once the mqtt callback is called
	 */
	struct lua_state_t *state = plua_get_free_state();
	state->module = data->module;

	logprintf(LOG_DEBUG, "lua mqtt on state #%d", state->idx);

	switch(state->module->type) {
		case FUNCTION: {
			sprintf(p, "function.%s", state->module->name);
		} break;
		case OPERATOR: {
			sprintf(p, "operator.%s", state->module->name);
		} break;
		case ACTION: {
			sprintf(p, "action.%s", state->module->name);
		} break;
	}

	lua_getglobal(state->L, name);
	if(lua_type(state->L, -1) == LUA_TNIL) {
		luaL_error(state->L, "cannot find %s lua module", name);
	}

	lua_getfield(state->L, -1, data->callback);

	if(lua_type(state->L, -1) != LUA_TFUNCTION) {
		luaL_error(state->L, "%s: mqtt callback %s does not exist", state->module->file, data->callback);
	}

	plua_network_mqtt_object(state->L, data);

	data->status = status;

	if(lua_pcall(state->L, 1, 0, 0) == LUA_ERRRUN) {
		if(lua_type(state->L, -1) == LUA_TNIL) {
			logprintf(LOG_ERR, "%s: syntax error", state->module->file);
			goto error;
		}
		if(lua_type(state->L, -1) == LUA_TSTRING) {
			logprintf(LOG_ERR, "%s", lua_tostring(state->L,  -1));
			lua_pop(state->L, -1);
			plua_clear_state(state);
			goto error;
		}
	}
	lua_remove(state->L, 1);
	plua_clear_state(state);

error:
	FREE(mqtt->message);
	FREE(mqtt->topic);
	FREE(mqtt->clientid);

	plua_metatable_free(data->table);
	FREE(data->password);
	FREE(data->callback);
	FREE(data->user);
	FREE(data->host);
	FREE(data);
}

static int plua_network_mqtt_send(lua_State *L) {
	struct lua_mqtt_t *mqtt = (void *)lua_topointer(L, lua_upvalueindex(1));

	if(lua_gettop(L) != 0) {
		luaL_error(L, "mqtt.send requires 0 arguments, %d given", lua_gettop(L));
	}

	if(mqtt == NULL) {
		luaL_error(L, "internal error: mqtt object not passed");
	}

	if(mqtt->host == NULL) {
		luaL_error(L, "mqtt host not set");
	}

	if(mqtt->user == NULL) {
		luaL_error(L, "mqtt user not set");
	}

	if(mqtt->password == NULL) {
		luaL_error(L, "mqtt password not set");
	}

	if(mqtt->port == 0) {
		luaL_error(L, "mqtt  port not set");
	}

	if(mqtt->mqtt.reqtype == 0) {
		luaL_error(L, "mqtt  request type not set");
	}

	if(mqtt->mqtt.topic == NULL) {
		luaL_error(L, "mqtt topic not set");
	}

	if(mqtt->mqtt.clientid == NULL) {
		luaL_error(L, "mqtt client id not set");
	}

	if(mqtt->mqtt.message == NULL) {
		luaL_error(L, "mqtt message not set");
	}

	if(mqtt->callback == NULL) {
		luaL_error(L, "mqtt callback not set");
	}

	mqtt->mqtt.data = mqtt;
	
	if(mqtt_request(
		mqtt->host,
		mqtt->user,
		mqtt->password,
		mqtt->port,
		mqtt->is_ssl,
		&mqtt->mqtt, 
		plua_network_mqtt_callback) != 0) {

		plua_gc_unreg(mqtt->L, mqtt);
		plua_network_mqtt_gc((void *)mqtt);

		lua_pushboolean(L, 0);
		assert(lua_gettop(L) == 1);

		return 1;
	}

	plua_gc_unreg(mqtt->L, mqtt);

	lua_pushboolean(L, 1);
	assert(lua_gettop(L) == 1);

	return 1;
}

static int plua_network_mqtt_get_status(lua_State *L) {
	struct lua_mqtt_t *mqtt = (void *)lua_topointer(L, lua_upvalueindex(1));

	if(lua_gettop(L) != 0) {
		luaL_error(L, "mqtt.getStatus requires 0 arguments, %d given", lua_gettop(L));
	}

	if(mqtt == NULL) {
		luaL_error(L, "internal error: mqtt object not passed");
	}

	switch(mqtt->status) {
		case -1:
			lua_pushboolean(L, 0);
		break;
		case 0:
			lua_pushboolean(L, 1);
		break;
		case 1:
			lua_pushnil(L);
		break;
	}

	assert(lua_gettop(L) == 1);

	return 1;
}

static int plua_network_mqtt_get_topic(lua_State *L) {
	struct lua_mqtt_t *mqtt = (void *)lua_topointer(L, lua_upvalueindex(1));

	if(lua_gettop(L) != 0) {
		luaL_error(L, "mqtt.getTopic requires 0 arguments, %d given", lua_gettop(L));
	}

	if(mqtt == NULL) {
		luaL_error(L, "internal error: mqtt object not passed");
	}

	if(mqtt->mqtt.topic != NULL) {
		lua_pushstring(L, mqtt->mqtt.topic);
	} else {
		lua_pushnil(L);
	}

	assert(lua_gettop(L) == 1);

	return 1;
}

static int plua_network_mqtt_get_clientid(lua_State *L) {
	struct lua_mqtt_t *mqtt = (void *)lua_topointer(L, lua_upvalueindex(1));

	if(lua_gettop(L) != 0) {
		luaL_error(L, "mqtt.getClientid requires 0 arguments, %d given", lua_gettop(L));
	}

	if(mqtt == NULL) {
		luaL_error(L, "internal error: mqtt object not passed");
	}

	if(mqtt->mqtt.clientid != NULL) {
		lua_pushstring(L, mqtt->mqtt.clientid);
	} else {
		lua_pushnil(L);
	}

	assert(lua_gettop(L) == 1);

	return 1;
}

static int plua_network_mqtt_get_message(lua_State *L) {
	struct lua_mqtt_t *mqtt = (void *)lua_topointer(L, lua_upvalueindex(1));

	if(lua_gettop(L) != 0) {
		luaL_error(L, "mqtt.getMessage requires 0 arguments, %d given", lua_gettop(L));
	}

	if(mqtt == NULL) {
		luaL_error(L, "internal error: mqtt object not passed");
	}

	if(mqtt->mqtt.message != NULL) {
		lua_pushstring(L, mqtt->mqtt.message);
	} else {
		lua_pushnil(L);
	}

	assert(lua_gettop(L) == 1);

	return 1;
}

static int plua_network_mqtt_get_host(lua_State *L) {
	struct lua_mqtt_t *mqtt = (void *)lua_topointer(L, lua_upvalueindex(1));

	if(lua_gettop(L) != 0) {
		luaL_error(L, "mqtt.getHost requires 0 arguments, %d given", lua_gettop(L));
	}

	if(mqtt == NULL) {
		luaL_error(L, "internal error: mqtt object not passed");
	}

	if(mqtt->host != NULL) {
		lua_pushstring(L, mqtt->host);
	} else {
		lua_pushnil(L);
	}

	assert(lua_gettop(L) == 1);

	return 1;
}

static int plua_network_mqtt_get_user(lua_State *L) {
	struct lua_mqtt_t *mqtt = (void *)lua_topointer(L, lua_upvalueindex(1));

	if(lua_gettop(L) != 0) {
		luaL_error(L, "mqtt.getUser requires 0 arguments, %d given", lua_gettop(L));
	}

	if(mqtt == NULL) {
		luaL_error(L, "internal error: mqtt object not passed");
	}

	if(mqtt->user != NULL) {
		lua_pushstring(L, mqtt->user);
	} else {
		lua_pushnil(L);
	}

	assert(lua_gettop(L) == 1);

	return 1;
}

static int plua_network_mqtt_get_password(lua_State *L) {
	struct lua_mqtt_t *mqtt = (void *)lua_topointer(L, lua_upvalueindex(1));

	if(lua_gettop(L) != 0) {
		luaL_error(L, "mqtt.getPassword requires 0 arguments, %d given", lua_gettop(L));
	}

	if(mqtt == NULL) {
		luaL_error(L, "internal error: mqtt object not passed");
	}

	if(mqtt->password != NULL) {
		lua_pushstring(L, mqtt->password);
	} else {
		lua_pushnil(L);
	}

	assert(lua_gettop(L) == 1);

	return 1;
}

static int plua_network_mqtt_get_qos(lua_State *L) {
	struct lua_mqtt_t *mqtt = (void *)lua_topointer(L, lua_upvalueindex(1));

	if(lua_gettop(L) != 0) {
		luaL_error(L, "mqtt.getQos requires 0 arguments, %d given", lua_gettop(L));
	}

	if(mqtt == NULL) {
		luaL_error(L, "internal error: mqtt object not passed");
	}

	lua_pushnumber(L, mqtt->mqtt.qos);

	assert(lua_gettop(L) == 1);

	return 1;
}

static int plua_network_mqtt_get_ssl(lua_State *L) {
	struct lua_mqtt_t *mqtt = (void *)lua_topointer(L, lua_upvalueindex(1));

	if(lua_gettop(L) != 0) {
		luaL_error(L, "mqtt.getSSL requires 0 arguments, %d given", lua_gettop(L));
	}

	if(mqtt == NULL) {
		luaL_error(L, "internal error: mqtt object not passed");
	}

	lua_pushnumber(L, mqtt->is_ssl);

	assert(lua_gettop(L) == 1);

	return 1;
}
static int plua_network_mqtt_get_reqtype(lua_State *L) {
	struct lua_mqtt_t *mqtt = (void *)lua_topointer(L, lua_upvalueindex(1));

	if(lua_gettop(L) != 0) {
		luaL_error(L, "mqtt.getMsgtype requires 0 arguments, %d given", lua_gettop(L));
	}

	if(mqtt == NULL) {
		luaL_error(L, "internal error: mqtt object not passed");
	}

	lua_pushnumber(L, mqtt->mqtt.reqtype);

	assert(lua_gettop(L) == 1);

	return 1;
}

static int plua_network_mqtt_get_port(lua_State *L) {
	struct lua_mqtt_t *mqtt = (void *)lua_topointer(L, lua_upvalueindex(1));

	if(lua_gettop(L) != 0) {
		luaL_error(L, "mqtt.getPort requires 0 arguments, %d given", lua_gettop(L));
	}

	if(mqtt == NULL) {
		luaL_error(L, "internal error: mqtt object not passed");
	}

	lua_pushnumber(L, mqtt->port);

	assert(lua_gettop(L) == 1);

	return 1;
}

static int plua_network_mqtt_get_callback(lua_State *L) {
	struct lua_mqtt_t *mqtt = (void *)lua_topointer(L, lua_upvalueindex(1));

	if(lua_gettop(L) != 0) {
		luaL_error(L, "mqtt.getSSL requires 0 arguments, %d given", lua_gettop(L));
	}

	if(mqtt == NULL) {
		luaL_error(L, "internal error: mqtt object not passed");
	}

	if(mqtt->callback != NULL) {
		lua_pushstring(L, mqtt->callback);
	} else {
		lua_pushnil(L);
	}

	assert(lua_gettop(L) == 1);

	return 1;
}

static void plua_network_mqtt_gc(void *ptr) {
	struct lua_mqtt_t *data = ptr;

	if(data->mqtt.clientid != NULL) {
		FREE(data->mqtt.clientid);
	}
	if(data->mqtt.message != NULL) {
		FREE(data->mqtt.message);
	}
	if(data->mqtt.topic != NULL) {
		FREE(data->mqtt.topic);
	}
	
	plua_metatable_free(data->table);

	if(data->password != NULL) {
		FREE(data->password);
	}
	if(data->callback != NULL) {
		FREE(data->callback);
	}
	if(data->user != NULL) {
		FREE(data->user);
	}
	if(data->host != NULL) {
		FREE(data->host);
	}
	FREE(data);
}

static void plua_network_mqtt_object(lua_State *L, struct lua_mqtt_t *mqtt) {
	lua_newtable(L);

	lua_pushstring(L, "setTopic");
	lua_pushlightuserdata(L, mqtt);
	lua_pushcclosure(L, plua_network_mqtt_set_topic, 1);
	lua_settable(L, -3);

	lua_pushstring(L, "getTopic");
	lua_pushlightuserdata(L, mqtt);
	lua_pushcclosure(L, plua_network_mqtt_get_topic, 1);
	lua_settable(L, -3);

	lua_pushstring(L, "setReqtype");
	lua_pushlightuserdata(L, mqtt);
	lua_pushcclosure(L, plua_network_mqtt_set_reqtype, 1);
	lua_settable(L, -3);

	lua_pushstring(L, "getReqtype");
	lua_pushlightuserdata(L, mqtt);
	lua_pushcclosure(L, plua_network_mqtt_get_reqtype, 1);
	lua_settable(L, -3);

	lua_pushstring(L, "setClientid");
	lua_pushlightuserdata(L, mqtt);
	lua_pushcclosure(L, plua_network_mqtt_set_clientid, 1);
	lua_settable(L, -3);

	lua_pushstring(L, "getClientid");
	lua_pushlightuserdata(L, mqtt);
	lua_pushcclosure(L, plua_network_mqtt_get_clientid, 1);
	lua_settable(L, -3);

	lua_pushstring(L, "setMessage");
	lua_pushlightuserdata(L, mqtt);
	lua_pushcclosure(L, plua_network_mqtt_set_message, 1);
	lua_settable(L, -3);

	lua_pushstring(L, "getMessage");
	lua_pushlightuserdata(L, mqtt);
	lua_pushcclosure(L, plua_network_mqtt_get_message, 1);
	lua_settable(L, -3);

	lua_pushstring(L, "setHost");
	lua_pushlightuserdata(L, mqtt);
	lua_pushcclosure(L, plua_network_mqtt_set_host, 1);
	lua_settable(L, -3);

	lua_pushstring(L, "getHost");
	lua_pushlightuserdata(L, mqtt);
	lua_pushcclosure(L, plua_network_mqtt_get_host, 1);
	lua_settable(L, -3);

	lua_pushstring(L, "setPort");
	lua_pushlightuserdata(L, mqtt);
	lua_pushcclosure(L, plua_network_mqtt_set_port, 1);
	lua_settable(L, -3);

	lua_pushstring(L, "getPort");
	lua_pushlightuserdata(L, mqtt);
	lua_pushcclosure(L, plua_network_mqtt_get_port, 1);
	lua_settable(L, -3);

	lua_pushstring(L, "setUser");
	lua_pushlightuserdata(L, mqtt);
	lua_pushcclosure(L, plua_network_mqtt_set_user, 1);
	lua_settable(L, -3);

	lua_pushstring(L, "getUser");
	lua_pushlightuserdata(L, mqtt);
	lua_pushcclosure(L, plua_network_mqtt_get_user, 1);
	lua_settable(L, -3);

	lua_pushstring(L, "setPassword");
	lua_pushlightuserdata(L, mqtt);
	lua_pushcclosure(L, plua_network_mqtt_set_password, 1);
	lua_settable(L, -3);

	lua_pushstring(L, "getPassword");
	lua_pushlightuserdata(L, mqtt);
	lua_pushcclosure(L, plua_network_mqtt_get_password, 1);
	lua_settable(L, -3);

	lua_pushstring(L, "setQos");
	lua_pushlightuserdata(L, mqtt);
	lua_pushcclosure(L, plua_network_mqtt_set_qos, 1);
	lua_settable(L, -3);

	lua_pushstring(L, "getQos");
	lua_pushlightuserdata(L, mqtt);
	lua_pushcclosure(L, plua_network_mqtt_get_qos, 1);
	lua_settable(L, -3);

	lua_pushstring(L, "setSSL");
	lua_pushlightuserdata(L, mqtt);
	lua_pushcclosure(L, plua_network_mqtt_set_ssl, 1);
	lua_settable(L, -3);

	lua_pushstring(L, "getSSL");
	lua_pushlightuserdata(L, mqtt);
	lua_pushcclosure(L, plua_network_mqtt_get_ssl, 1);
	lua_settable(L, -3);

	lua_pushstring(L, "setCallback");
	lua_pushlightuserdata(L, mqtt);
	lua_pushcclosure(L, plua_network_mqtt_set_callback, 1);
	lua_settable(L, -3);

	lua_pushstring(L, "getCallback");
	lua_pushlightuserdata(L, mqtt);
	lua_pushcclosure(L, plua_network_mqtt_get_callback, 1);
	lua_settable(L, -3);

	lua_pushstring(L, "send");
	lua_pushlightuserdata(L, mqtt);
	lua_pushcclosure(L, plua_network_mqtt_send, 1);
	lua_settable(L, -3);

	lua_pushstring(L, "getStatus");
	lua_pushlightuserdata(L, mqtt);
	lua_pushcclosure(L, plua_network_mqtt_get_status, 1);
	lua_settable(L, -3);

	lua_pushstring(L, "getUserdata");
	lua_pushlightuserdata(L, mqtt);
	lua_pushcclosure(L, plua_network_mqtt_get_data, 1);
	lua_settable(L, -3);

	lua_pushstring(L, "setUserdata");
	lua_pushlightuserdata(L, mqtt);
	lua_pushcclosure(L, plua_network_mqtt_set_data, 1);
	lua_settable(L, -3);
}

int plua_network_mqtt(struct lua_State *L) {
	if(lua_gettop(L) != 0) {
		luaL_error(L, "timer requires 0 arguments, %d given", lua_gettop(L));
		return 0;
	}

	struct lua_state_t *state = plua_get_current_state(L);
	if(state == NULL) {
		return 0;
	}

	struct lua_mqtt_t *lua_mqtt = MALLOC(sizeof(struct lua_mqtt_t));
	if(lua_mqtt == NULL) {
		OUT_OF_MEMORY
	}
	memset(lua_mqtt, '\0', sizeof(struct lua_mqtt_t));

	if((lua_mqtt->table = MALLOC(sizeof(struct lua_mqtt_t))) == NULL) {
		OUT_OF_MEMORY /*LCOV_EXCL_LINE*/
	}
	memset(lua_mqtt->table, 0, sizeof(struct lua_mqtt_t));

	lua_mqtt->module = state->module;
	lua_mqtt->L = L;
	lua_mqtt->status = 1;

	plua_gc_reg(L, lua_mqtt, plua_network_mqtt_gc);

	plua_network_mqtt_object(L, lua_mqtt);

	lua_assert(lua_gettop(L) == 1);

	return 1;
}
