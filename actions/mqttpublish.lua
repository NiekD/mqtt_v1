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

	if parameters['MESSAGE'] == nil then
		error("mqtt action is missing a \"MESSAGE\" statement");
	end
	
	if parameters['BROKER'] == nil then
		error("mqtt action is missing a \"BROKER\" statement");
	end

	if #parameters['TOPIC']['value'] ~= 1 or parameters['TOPIC']['value'][2] ~= nil then
		error("mqtt action \"TOPIC\" only takes one argument");
	end

	if #parameters['MESSAGE']['value'] ~= 1 or parameters['MESSAGE']['value'][2] ~= nil then
		error("mqtt action \"MESSAGE\" only takes one argument");
	end
	
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

function M.callback(mqtt)
	if mqtt.getStatus() == true then
		error("successfully published topic \"" .. mqtt.getTopic() .. "\" with Qos" .. tostring(mqtt.getQos()));
	else
		error("failed to publish topic \"" .. mqtt.getTopic() .. "\" with Qos" .. tostring(mqtt.getQos()));
	end
end

function M.run(parameters)
	local topic = parameters['TOPIC']['value'][1];
	local message = parameters['MESSAGE']['value'][1];
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
	mqttobj.setReqtype(3) -- PUBLISH
	mqttobj.setTopic(topic);
	mqttobj.setMessage(message);
	mqttobj.setQos(tonumber(qos));
	mqttobj.setHost(broker);
	mqttobj.setPort(tonumber(port));
	mqttobj.setUser("none");
	mqttobj.setPassword("none");
	mqttobj.setClientid("pilight");
	mqttobj.setCallback("callback");
	
	mqttobj.send();

	return 1;
end

function M.parameters()
	return "TOPIC", "MESSAGE", "QOS", "BROKER", "PORT";
end

function M.info()
	return {
		name = "mqttpublish",
		version = "1.0",
		reqversion = "8.0",
		reqcommit = ""
	}
end

return M;
