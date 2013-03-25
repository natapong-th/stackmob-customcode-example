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

public class DeleteEvents implements CustomCodeMethod {

	@Override
	public String getMethodName() {
		return "delete_events";
	}
	
	@Override
	public List<String> getParams() {
		return Arrays.asList("event_ids");
	}
	
	@Override
	public ResponseToProcess execute(ProcessedAPIRequest request, SDKServiceProvider serviceProvider) {
		// only allow PUT method
		String verb = request.getVerb().toString();
		if (!verb.equalsIgnoreCase("put")) {
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
		List<SMString> eventIds = new ArrayList<SMString>();
		if (!request.getBody().isEmpty()) {
			try {
				JSONObject jsonObj = new JSONObject(request.getBody());
				if (!jsonObj.isNull("event_ids")) {
					JSONArray eventArray = jsonObj.getJSONArray("event_ids");
					for (int i = 0; i < eventArray.length(); i++) {
						String eventId = eventArray.getString(i);
						eventIds.add(new SMString(eventId));
					}
				}
			} catch (JSONException e) {
				HashMap<String, String> errParams = new HashMap<String, String>();
				errParams.put("error", "invalid request body");
				return new ResponseToProcess(HttpURLConnection.HTTP_BAD_REQUEST, errParams); // http 400 - bad request
			}
		}
		if (eventIds.size() == 0) {
			HashMap<String, String> errParams = new HashMap<String, String>();
			errParams.put("error", "usernames parameter not found");
			return new ResponseToProcess(HttpURLConnection.HTTP_BAD_REQUEST, errParams); // http 400 - bad request
		}
		
		// get the datastore service
		DataService dataService = serviceProvider.getDataService();
		
		// create a response
		try {
			// fetch event object
			// - build query
			List<SMCondition> eventQuery = new ArrayList<SMCondition>();
			eventQuery.add(new SMIn("event_id", eventIds));
			// - build result filter
			List<String> fields = new ArrayList<String>();
			fields.add("event_id");
			fields.add("relationship_by_owner");
			fields.add("relationship_by_owner.relationship_id");
			fields.add("relationship_by_owner.receiver");
			fields.add("relationship_by_receiver");
			fields.add("relationship_by_receiver.relationship_id");
			fields.add("relationship_by_receiver.owner");
			ResultFilters filter = new ResultFilters(0, -1, null, fields);
			// - execute query
			List<SMObject> events = dataService.readObjects("event", eventQuery, 1, filter);
			// report error if query failed
			if (events == null || events.size() != eventIds.size()) {
				HashMap<String, String> errMap = new HashMap<String, String>();
				errMap.put("error", "invalid event fetch");
				errMap.put("detail", (events == null ? "null fetch result" : ("fetch result count = " + events.size())));
				return new ResponseToProcess(HttpURLConnection.HTTP_INTERNAL_ERROR, errMap);
			}
			
			Map<String, Object> returnMap = new HashMap<String, Object>();
			List<SMString> removedEvents = new ArrayList<SMString>();
			for (int i = 0; i < events.size(); i++) {
				SMObject eventObject = events.get(i);
				SMString eventId = (SMString)eventObject.getValue().get("event_id");
				// find event user's role
				String userRole = "owner";
				String creatorRole = "receiver";
				if (eventObject.getValue().containsKey("relationship_by_owner")) {
					userRole = "receiver";
					creatorRole = "owner";
				}
				
				// check if this user is the receiver of this event
				SMObject relObject = (SMObject)eventObject.getValue().get("relationship_by_" + creatorRole);
				SMString relId = (SMString)relObject.getValue().get("relationship_id");
				SMString receiverId = (SMString)relObject.getValue().get(userRole);
				if (!receiverId.equals(userId)) {
					continue;
				}
				
				// remove & delete the event from the relationship
				List<SMString> eventIdList = new ArrayList<SMString>();
				eventIdList.add(eventId);
				dataService.removeRelatedObjects("relationship", relId, "events_by_" + creatorRole, eventIdList, true);
				
				removedEvents.add(eventId);
			}
			returnMap.put("event_ids", removedEvents);
			
			// return updated data for local database
			long currentTime = System.currentTimeMillis();
			returnMap.put("last_sync_date", new Long(currentTime));
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
