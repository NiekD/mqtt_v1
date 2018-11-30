--
-- Copyright (C) 2018 CurlyMo & Niek
--
-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--

local M = {}

function M.check(parameters)
	if parameters['TOPIC'] == nil then
		error("mqtt action is missing a \"TOPIC\" statement");
	end

--	if parameters['DEVICE'] == nil then
--		error("mqtt action is missing a \"DEVICE\" statement");
--	end
	
	if parameters['BROKER'] == nil then
		error("mqtt action is missing a \"BROKER\" statement");
	end

	if #parameters['TOPIC']['value'] ~= 1 or parameters['TOPIC']['value'][2] ~= nil then
		error("mqtt action \"TOPIC\" only takes one argument");
	end

--	if #parameters['DEVICE']['value'] ~= 1 or parameters['DEVICE']['value'][2] ~= nil then
--		error("mqtt action \"DEVICE\" only takes one argument");
--	end
	
	if parameters['PORT'] ~= nil then
		if #parameters['PORT']['value'] ~= 1 or parameters['PORT']['value'][2] ~= nil then
			error("mqtt action \"PORT\" only takes one argument");
		else if tonumber(parameters['PORT']['value'][1]) == nil then
			error("mqtt action \"PORT\" must be a number");
		end
	end
		
	end	if parameters['QOS'] ~= nil then
		if #parameters['QOS']['value'] ~= 1 or parameters['QOS']['value'][2] ~= nil then
			error("mqtt action \"QOS\" only takes one argument");
		else if tonumber(parameters['QOS']['value'][1]) < 0 or tonumber(parameters['QOS']['value'][1]) > 2 then
			error("mqtt action \"QOS\" can only be 0, 1 or 2");
			end
		end
	end
	
	if #parameters['BROKER']['value'] ~= 1 or parameters['BROKER']['value'][2] ~= nil then
		error("mqtt action \"BROKER\" only takes one argument");
	end
	
	return 1;
end

function M.thread(thread)
	local data = thread.getUserdata();
	local devname = data['device'];
	local config = pilight.config();
	local devobj = config.getDevice(devname);

	if devobj.setState(data['new_state']) == false then
		error("device \"" .. devname .. "\" could not be set to state \"" .. data['new_state'] .. "\"")
	end

	devobj.send();
end

function M.callback(mqtt)
	if mqtt.getStatus() == true then
	--error("FD=" .. tostring(mqtt.getFd()) .. ", connected=" .. mqtt.getConnected() .. ", subscribed=" .. mqtt.getSubscribed());
		if string.len(mqtt.getMessage()) > 0 then
			local topicparts = pilight.common.explode(mqtt.getTopic(), "/");
			local base = topicparts[1];
			local cmdtype = topicparts[2];
--			if #topicparts == 3 and base == "pilight" and cmdtype == "setdevice" then
			local devname = topicparts[#topicparts];
			local config = pilight.config();
			local devobj = config.getDevice(devname);
			
			if devobj ~= nil then
				local old_state = nil;
				local new_state = mqtt.getMessage();
				local async = pilight.async.thread();
				
				if devobj.hasSetting("state") == true then
					if devobj.getState ~= nil then
						old_state = devobj.getState();
					end
				else
					error("mqtt device \"" .. devname .. "\" doesn't have a state (topic \"".. mqtt.getTopic() .. "\")");		
				end
				
				if old_state ~= new_state then 
					local data = async.getUserdata();

					data['device'] = devname;
					data['old_state'] = old_state;
					data['new_state'] = new_state;
					async.setCallback("thread");
					async.trigger();
					
					error("mqtt switched device \"" .. devname .. "\" to \"" ..  mqtt.getMessage() .. "\"");
				else
					error("mqtt device \"" .. devname .. "\" not switched, because it was already \"" ..  mqtt.getMessage() .. "\"");
				end
				
			else
				error("mqtt received message, but topic \"" .. mqtt.getTopic() .. "\" does not refer to an existing pilight device");
			end
		else
--			error("mqtt no new message available");		
		end	
	else
		error("mqtt responded with error status");
	end
end

function M.run(parameters)
	local topic = parameters['TOPIC']['value'][1];
--	local device = parameters['DEVICE']['value'][1];
	local broker = parameters['BROKER']['value'][1];
	local qos = 0;
	local port = 1883;
	if parameters['QOS'] ~= nil then
		qos = parameters['QOS']['value'][1];
	end
	if parameters['PORT'] ~= nil then
		port = parameters['PORT']['value'][1];
	end

	local mqttobj = pilight.network.mqtt();
	mqttobj.setReqtype(99); -- GETMESSAGE
	mqttobj.setTopic(topic);
	mqttobj.setQos(tonumber(qos));
	mqttobj.setMessage("");
	mqttobj.setHost(broker);
	mqttobj.setPort(tonumber(port));
	mqttobj.setUser("none");
	mqttobj.setPassword("none");
	mqttobj.setClientid("pilight");
--	mqttobj.setDevice(device);	
	mqttobj.setCallback("callback");
	
	mqttobj.send();

	return 1;
end

function M.parameters()
	return "TOPIC", "QOS", "BROKER", "PORT", "DEVICE";
end

function M.info()
	return {
		name = "mqttgetmsg",
		version = "1.0",
		reqversion = "8.0",
		reqcommit = ""
	}
end

return M;