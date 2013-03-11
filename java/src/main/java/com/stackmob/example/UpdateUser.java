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

public class UpdateUser implements CustomCodeMethod {

	@Override
	public String getMethodName() {
		return "update_user";
	}
	
	@Override
	public List<String> getParams() {
		return Arrays.asList("name", "profile_image_url", "group_order");
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
		String name = "";
		boolean newName = false;
		String profileImageURL = "";
		boolean newImage = false;
		String groupOrder = "";
		boolean newOrder = false;
		if (!request.getBody().isEmpty()) {
			try {
				JSONObject jsonObj = new JSONObject(request.getBody());
				if (!jsonObj.isNull("name")) {
					name = jsonObj.getString("name");
					newName = true;
				}
				if (!jsonObj.isNull("profile_image_url")) {
					profileImageURL = jsonObj.getString("profile_image_url");
					newImage = true;
				}
				if (!jsonObj.isNull("group_order")) {
					groupOrder = jsonObj.getString("group_order");
					newOrder = true;
				}
			} catch (JSONException e) {
				HashMap<String, String> errParams = new HashMap<String, String>();
				errParams.put("error", "invalid request body");
				return new ResponseToProcess(HttpURLConnection.HTTP_BAD_REQUEST, errParams); // http 400 - bad request
			}
		}
		if (!newName && !newImage && !newOrder) {
			HashMap<String, String> errParams = new HashMap<String, String>();
			errParams.put("error", "no parameters to update");
			return new ResponseToProcess(HttpURLConnection.HTTP_BAD_REQUEST, errParams); // http 400 - bad request
		}
		
		// get the datastore service
		DataService dataService = serviceProvider.getDataService();
		
		// create a response
		try {
			Map<String, Object> returnMap = new HashMap<String, Object>();
			List<SMUpdate> userUpdates = new ArrayList<SMUpdate>();
			// 1. change name
			if (newName) {
				userUpdates.add(new SMSet("name", new SMString(name)));
				returnMap.put("name", name);
			}
			// 2. change profile url
			if (newImage) {
				userUpdates.add(new SMSet("profile_image_url", new SMString(profileImageURL)));
				returnMap.put("profile_image_url", profileImageURL);
			}
			// 3. change group order
			if (newOrder) {
				// fetch user object
				// - build query
				List<SMCondition> userQuery = new ArrayList<SMCondition>();
				userQuery.add(new SMEquals("username", userId));
				// - execute query
				List<SMObject> users = dataService.readObjects("user", userQuery);
				if (users != null && users.size() == 1) {
					SMObject userObject = users.get(0);
					// check if group order is valid
					String[] groupOrderArray = groupOrder.split("|");
					List<SMString> groupsList = new ArrayList<SMString>();
					if (userObject.getValue().containsKey("groups")) {
						SMValue groupsValue = userObject.getValue().get("groups");
						groupsList = ((SMList<SMString>)groupsValue).getValue();
					}
					boolean validOrder = (groupsList.size() == groupOrderArray.length);
					if (validOrder) {
						for (int i = 0; i < groupsList.size(); i++) {
							String groupId = groupsList.get(i).getValue();
							boolean found = false;
							for (int j = 0; j < groupOrderArray.length; j++) {
								if (groupOrderArray[j].equals(groupId)) {
									found = true;
									break;
								}
							}
							if (!found) {
								validOrder = false;
								break;
							}
						}
					}
					if (validOrder) {
						userUpdates.add(new SMSet("group_order", new SMString(groupOrder)));
						returnMap.put("group_order", groupOrder);
					} else {
						HashMap<String, String> errParams = new HashMap<String, String>();
						errParams.put("error", "invalid group order");
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
			}
			// update user
			dataService.updateObject("user", userId, userUpdates);
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
