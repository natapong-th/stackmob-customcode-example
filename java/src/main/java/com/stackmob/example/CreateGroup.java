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

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class CreateGroup implements CustomCodeMethod {

	@Override
	public String getMethodName() {
		return "create_group";
	}
	
	@Override
	public List<String> getParams() {
		return Arrays.asList("title");
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
		SMString userId = new SMString(username);
		if (username == null || username.isEmpty()) {
			HashMap<String, String> errParams = new HashMap<String, String>();
			errParams.put("error", "no user is logged in");
			return new ResponseToProcess(HttpURLConnection.HTTP_UNAUTHORIZED, errParams); // http 401 - unauthorized
		}
		
		// get requested group title
		String title = "";
		if (!request.getBody().isEmpty()) {
			try {
				JSONObject jsonObj = new JSONObject(request.getBody());
				if (!jsonObj.isNull("title")) {
					title = jsonObj.getString("title");
				}
			} catch (JSONException e) {
				HashMap<String, String> errParams = new HashMap<String, String>();
				errParams.put("error", "invalid request body");
				return new ResponseToProcess(HttpURLConnection.HTTP_BAD_REQUEST, errParams); // http 400 - bad request
			}
		}
		if (title == null || title.isEmpty()) {
			HashMap<String, String> errParams = new HashMap<String, String>();
			errParams.put("error", "title parameter not found");
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
			// - execute query
			List<SMObject> users = dataService.readObjects("user", userQuery);
			if (users != null && users.size() == 1) {
				SMObject userObject = users.get(0);
				// create a new group
				Map<String, SMValue> groupMap = new HashMap<String, SMValue>();
				groupMap.put("sm_owner", new SMString("user/" + username));
				groupMap.put("title", new SMString(title));
				groupMap.put("relationship_order", new SMString(""));
				SMObject groupObject = dataService.createObject("group", new SMObject(groupMap));
				SMString groupId = (SMString)groupObject.getValue().get("group_id");
				
				// add group in user's groups
				List<SMString> groupIdList = new ArrayList<SMString>();
				groupIdList.add(groupId);
				dataService.addRelatedObjects("user", userId, "groups", groupIdList);
				
				// add user as group's owner
				List<SMString> ownerIdList = new ArrayList<SMString>();
				ownerIdList.add(userId);
				dataService.addRelatedObjects("group", groupId, "owner", ownerIdList);
				
				// update user's group order
				List<SMUpdate> userUpdates = new ArrayList<SMUpdate>();
				if (userObject.getValue().containsKey("group_order")) {
					String groupOrder = ((SMString)userObject.getValue().get("group_order")).getValue();
					userUpdates.add(new SMSet("group_order", new SMString(groupOrder + groupId.getValue() + "|")));
				} else {
					userUpdates.add(new SMSet("group_order", new SMString(groupId.getValue() + "|")));
				}
				dataService.updateObject("user", userId, userUpdates);
				
				// return created group data for local database
				Map<String, Object> returnMap = new HashMap<String, Object>();
				returnMap.put("group_id", groupId.getValue());
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
