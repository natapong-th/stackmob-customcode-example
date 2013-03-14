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
import java.lang.System;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

// *** DEPRECATED***
public class UpdateStatus implements CustomCodeMethod {

	@Override
	public String getMethodName() {
		return "update_status";
	}
	
	@Override
	public List<String> getParams() {
		return Arrays.asList("action", "place");
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
		SMString userId = new SMString(username);
		if (username == null || username.isEmpty()) {
			HashMap<String, String> errParams = new HashMap<String, String>();
			errParams.put("error", "no user is logged in");
			return new ResponseToProcess(HttpURLConnection.HTTP_UNAUTHORIZED, errParams); // http 401 - unauthorized
		}
		
		// get update parameters
		String action = "";
		boolean newAction = false;
		String place = "";
		boolean newPlace = false;
		if (!request.getBody().isEmpty()) {
			try {
				JSONObject jsonObj = new JSONObject(request.getBody());
				if (!jsonObj.isNull("action")) {
					action = jsonObj.getString("action");
					newAction = true;
				}
				if (!jsonObj.isNull("place")) {
					place = jsonObj.getString("place");
					newPlace = true;
				}
			} catch (JSONException e) {
				HashMap<String, String> errParams = new HashMap<String, String>();
				errParams.put("error", "invalid request body");
				return new ResponseToProcess(HttpURLConnection.HTTP_BAD_REQUEST, errParams); // http 400 - bad request
			}
		}
		if (!newAction && !newPlace) {
			HashMap<String, String> errParams = new HashMap<String, String>();
			errParams.put("error", "no parameters to update");
			return new ResponseToProcess(HttpURLConnection.HTTP_BAD_REQUEST, errParams); // http 400 - bad request
		}
		
		// get the datastore service
		DataService dataService = serviceProvider.getDataService();
		
		// create a response
		try {
			// fetch user object
			// - build query
			List<SMCondition> userQuery = new ArrayList<SMCondition>();
			userQuery.add(new SMEquals("username", userId));
			// - build result filter
			List<String> fields = new ArrayList<String>();
			fields.add("status");
			fields.add("status.status_id");
			fields.add("status.action");
			fields.add("status.place");
			ResultFilters filter = new ResultFilters(0, -1, null, fields);
			// - execute query
			List<SMObject> users = dataService.readObjects("user", userQuery, 1, filter);
			if (users != null && users.size() == 1) {
				SMObject userObject = users.get(0);
				if (userObject.getValue().containsKey("status")) {
					Map<String, Object> returnMap = new HashMap<String, Object>();
					List<SMUpdate> statusUpdates = new ArrayList<SMUpdate>();
					// check if action or place are different
					SMObject statusObject = (SMObject) userObject.getValue().get("status");
					SMString statusId = (SMString) statusObject.getValue().get("status_id");
					SMString oldAction = (SMString) statusObject.getValue().get("action");
					SMString oldPlace = (SMString) statusObject.getValue().get("place");
					boolean changed = false;
					// 1. change action
					if (newAction && !oldAction.getValue().equals(action)) {
						statusUpdates.add(new SMSet("action", new SMString(action)));
						returnMap.put("action", action);
						changed = true;
					}
					// 2. change place
					if (newPlace && !oldPlace.getValue().equals(place)) {
						statusUpdates.add(new SMSet("place", new SMString(place)));
						returnMap.put("place", place);
						changed = true;
					}
					if (changed) {
						// 3. change mod date
						long currentTime = System.currentTimeMillis();
						statusUpdates.add(new SMSet("status_mod_date", new SMInt(currentTime)));
						returnMap.put("status_mod_date", currentTime);
						// update status
						dataService.updateObject("status", statusId, statusUpdates);
					}
					// return updated data for local database
					return new ResponseToProcess(HttpURLConnection.HTTP_OK, returnMap);
				} else {
					HashMap<String, String> errParams = new HashMap<String, String>();
					errParams.put("error", "status not found");
					return new ResponseToProcess(HttpURLConnection.HTTP_BAD_REQUEST, errParams); // http 400 - bad request
				}
			} else {
				// TO DO:
				// handle user fetch error
				
				HashMap<String, String> errMap = new HashMap<String, String>();
				errMap.put("error", "invalid user fetch");
				errMap.put("detail", (users == null ? "null fetch result" : ("fetch result count = " + users.size())));
				return new ResponseToProcess(HttpURLConnection.HTTP_INTERNAL_ERROR, errMap);
			}
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
