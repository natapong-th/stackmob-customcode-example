/**
 * Copyright 2012-2013 StackMob
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.stackmob.example;

import com.stackmob.core.customcode.CustomCodeMethod;
import com.stackmob.core.rest.ProcessedAPIRequest;
import com.stackmob.core.rest.ResponseToProcess;
import com.stackmob.sdkapi.SDKServiceProvider;
import com.stackmob.sdkapi.*;

import com.stackmob.core.InvalidSchemaException;
import com.stackmob.core.DatastoreException;

import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Arrays;
import java.lang.String;
import java.lang.Long;
import java.lang.System;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class CreateStatusRequest implements CustomCodeMethod {

	@Override
	public String getMethodName() {
		return "create_status_request";
	}
	
	@Override
	public List<String> getParams() {
		return Arrays.asList("relationship_id");
	}
	
	@Override
	public ResponseToProcess execute(ProcessedAPIRequest request, SDKServiceProvider serviceProvider) {
		// only allow POST method
		String verb = request.getVerb().toString();
		if (!verb.equalsIgnoreCase("post")) {
			HashMap<String, String> errParams = new HashMap<String, String>();
			errParams.put("error", "invalid method");
			return new ResponseToProcess(HttpURLConnection.HTTP_BAD_METHOD, errParams); // http 405 - method not allowed
		}
		
		// try getting logged-in user
		String username = request.getLoggedInUser();
		if (username == null || username.isEmpty()) {
			HashMap<String, String> errParams = new HashMap<String, String>();
			errParams.put("error", "no user is logged in");
			return new ResponseToProcess(HttpURLConnection.HTTP_UNAUTHORIZED, errParams); // http 401 - unauthorized
		}
		SMString userId = new SMString(username);
		
		// get update parameters
		String relIdString = "";
		if (!request.getBody().isEmpty()) {
			try {
				JSONObject jsonObj = new JSONObject(request.getBody());
				if (!jsonObj.isNull("relationship_id")) {
					relIdString = jsonObj.getString("relationship_id");
				}
			} catch (JSONException e) {
				HashMap<String, String> errParams = new HashMap<String, String>();
				errParams.put("error", "invalid request body");
				return new ResponseToProcess(HttpURLConnection.HTTP_BAD_REQUEST, errParams); // http 400 - bad request
			}
		}
		if (relIdString.isEmpty()) {
			HashMap<String, String> errParams = new HashMap<String, String>();
			errParams.put("error", "invalid parameters");
			return new ResponseToProcess(HttpURLConnection.HTTP_BAD_REQUEST, errParams); // http 400 - bad request
		}
		SMString relId = new SMString(relIdString);
		
		// get the datastore service
		DataService dataService = serviceProvider.getDataService();
		
		// create a response
		try {
			// fetch relationship objects
			// - build query
			List<SMCondition> relQuery = new ArrayList<SMCondition>();
			relQuery.add(new SMEquals("relationship_id", relId));
			// - build result filter
			List<String> fields = new ArrayList<String>();
			fields.add("relationship_id");
			fields.add("type_by_owner");
			fields.add("type_by_receiver");
			fields.add("owner");
			fields.add("receiver");
			fields.add("events_by_owner");
			fields.add("events_by_owner.type");
			fields.add("events_by_receiver");
			fields.add("events_by_receiver.type");
			ResultFilters filter = new ResultFilters(0, -1, null, fields);
			// - execute query
			List<SMObject> rels = dataService.readObjects("relationship", relQuery, 1, filter);
			// report error if query failed
			if (rels == null || rels.size() != 1) {
				HashMap<String, String> errMap = new HashMap<String, String>();
				errMap.put("error", "invalid relationship fetch");
				errMap.put("detail", (rels == null ? "null fetch result" : ("fetch result count = " + rels.size())));
				return new ResponseToProcess(HttpURLConnection.HTTP_INTERNAL_ERROR, errMap);
			}
			
			SMObject relObject = rels.get(0);
			// find user's role in this relationship
			SMString ownerId = (SMString)relObject.getValue().get("owner");
			SMString receiverId = (SMString)relObject.getValue().get("receiver");
			String userRole = "";
			if (ownerId.equals(userId)) {
				userRole = "owner";
			} else if (receiverId.equals(userId)) {
				userRole = "receiver";
			}
			// check if user is in this relationship
			if (userRole.isEmpty()) {
				HashMap<String, String> errParams = new HashMap<String, String>();
				errParams.put("error", "requested relationship is inaccessible by this user");
				return new ResponseToProcess(HttpURLConnection.HTTP_BAD_REQUEST, errParams); // http 400 - bad request
			}
			
			Map<String, Object> returnMap = new HashMap<String, Object>();
			// only allow to request mutual friends (do not inform user)
			SMInt typeOwner = (SMInt)relObject.getValue().get("type_by_owner");
			SMInt typeReceiver = (SMInt)relObject.getValue().get("type_by_receiver");
			if (typeOwner.getValue().longValue() == 2L && typeReceiver.getValue().longValue() == 2L) {
				// check if this relationship already have a request event
				SMList<SMObject> eventsValue = (SMList<SMObject>)relObject.getValue().get("events_by_" + userRole);
				List<SMObject> events = eventsValue.getValue();
				boolean found = false;
				for (int i = 0; i < events.size(); i++) {
					SMObject eventObject = events.get(i);
					SMInt eventType = (SMInt)eventObject.getValue().get("type");
					if (eventType.getValue().longValue() == 3L) {
						found = true;
						break;
					}
				}
				
				if (!found) {
					Map<String, SMValue> eventMap = new HashMap<String, SMValue>();
					eventMap.put("sm_owner", new SMString("user/" + username));
					eventMap.put("type", new SMInt(3L));
					SMObject eventObject = dataService.createObject("event", new SMObject(eventMap));
					// get the new event id
					SMString eventId = (SMString)eventObject.getValue().get("relationship_id");
					// add event in relationship's events by user
					List<SMString> eventIdList = new ArrayList<SMString>();
					eventIdList.add(eventId);
					dataService.addRelatedObjects("relationship", relId, "events_by_" + userRole, eventIdList);
					// add relationship as event's relationship
					List<SMString> relIdList = new ArrayList<SMString>();
					relIdList.add(relId);
					dataService.addRelatedObjects("event", eventId, "relationship_by_" + userRole, relIdList);
					
					returnMap.put("event_id", eventId); 
				}
			}
			// return updated data for local database
			return new ResponseToProcess(HttpURLConnection.HTTP_OK, returnMap);
		} catch (InvalidSchemaException e) {
			HashMap<String, String> errMap = new HashMap<String, String>();
			errMap.put("error", "invalid_schema");
			errMap.put("detail", e.toString());
			return new ResponseToProcess(HttpURLConnection.HTTP_INTERNAL_ERROR, errMap); // http 500 - internal server error
		} catch (DatastoreException e) {
			HashMap<String, String> errMap = new HashMap<String, String>();
			errMap.put("error", "datastore_exception");
			errMap.put("detail", e.toString());
			return new ResponseToProcess(HttpURLConnection.HTTP_INTERNAL_ERROR, errMap); // http 500 - internal server error
		} catch (Exception e) {
			HashMap<String, String> errMap = new HashMap<String, String>();
			errMap.put("error", "unknown");
			errMap.put("detail", e.toString());
			return new ResponseToProcess(HttpURLConnection.HTTP_INTERNAL_ERROR, errMap); // http 500 - internal server error
		}
	}
}
