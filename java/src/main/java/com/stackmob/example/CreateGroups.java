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

// ***DEPRECATED***
public class CreateGroups implements CustomCodeMethod {

	@Override
	public String getMethodName() {
		return "create_groups";
	}
	
	@Override
	public List<String> getParams() {
		return Arrays.asList("titles");
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
		
		// get requested group titles
		List<String> titles = new ArrayList<String>();
		if (!request.getBody().isEmpty()) {
			try {
				JSONObject jsonObj = new JSONObject(request.getBody());
				if (!jsonObj.isNull("titles")) {
					JSONArray titleArray = jsonObj.getJSONArray("titles");
					// only get non-empty title
					for (int i = 0; i < titleArray.length(); i++) {
						String title = titleArray.getString(i);
						if (!title.isEmpty()) {
							titles.add(title);
						}
					}
				}
			} catch (JSONException e) {
				HashMap<String, String> errParams = new HashMap<String, String>();
				errParams.put("error", "invalid request body");
				return new ResponseToProcess(HttpURLConnection.HTTP_BAD_REQUEST, errParams); // http 400 - bad request
			}
		}
		if (titles.size() == 0) {
			HashMap<String, String> errParams = new HashMap<String, String>();
			errParams.put("error", "titles parameter not found");
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
			fields.add("group_order");
			ResultFilters filter = new ResultFilters(0, -1, null, fields);
			// - execute query
			List<SMObject> users = dataService.readObjects("user", userQuery, 0, filter);
			if (users != null && users.size() == 1) {
				SMObject userObject = users.get(0);
				List<SMString> groupIdList = new ArrayList<SMString>();
				for (int i = 0; i < titles.size(); i++) {
					String title = titles.get(i);
					// create a new group
					Map<String, SMValue> groupMap = new HashMap<String, SMValue>();
					groupMap.put("sm_owner", new SMString("user/" + username));
					groupMap.put("title", new SMString(title));
					SMObject groupObject = dataService.createObject("group", new SMObject(groupMap));
					// get the new group id
					SMString groupId = (SMString)groupObject.getValue().get("group_id");
					// store group id for adding in user's groups (and change group order)
					groupIdList.add(groupId);
					// add user as group's owner
					List<SMString> ownerIdList = new ArrayList<SMString>();
					ownerIdList.add(userId);
					dataService.addRelatedObjects("group", groupId, "owner", ownerIdList);
				}
				dataService.addRelatedObjects("user", userId, "groups", groupIdList);
				// update user's group order & groups mod date
				List<SMUpdate> userUpdates = new ArrayList<SMUpdate>();
				if (userObject.getValue().containsKey("group_order")) {
					List<SMString> groupOrder = ((SMList<SMString>)userObject.getValue().get("group_order")).getValue();
					groupOrder.addAll(groupIdList);
					userUpdates.add(new SMSet("group_order", new SMList<SMString>(groupOrder)));
				} else {
					userUpdates.add(new SMSet("group_order", new SMList<SMString>(groupIdList)));
				}
				long currentTime = System.currentTimeMillis();
				userUpdates.add(new SMSet("groups_mod_date", new SMInt(currentTime)));
				dataService.updateObject("user", userId, userUpdates);
				
				// return created group data for local database
				Map<String, Object> returnMap = new HashMap<String, Object>();
				returnMap.put("new_group_ids", groupIdList);
				returnMap.put("groups_mod_date", new Long(currentTime));
				return new ResponseToProcess(HttpURLConnection.HTTP_OK, returnMap);
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
