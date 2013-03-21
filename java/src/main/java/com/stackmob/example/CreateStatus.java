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
import java.lang.String;
import java.lang.System;

// ***DEPRECATED***
public class CreateStatus implements CustomCodeMethod {

	@Override
	public String getMethodName() {
		return "create_status";
	}
	
	@Override
	public List<String> getParams() {
		return new ArrayList<String>();
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
				// don't create new status if already have one
				if (userObject.getValue().containsKey("status") && ((SMString)userObject.getValue().get("status")).getValue() != "") {
					HashMap<String, String> errMap = new HashMap<String, String>();
					errMap.put("error", "status already exists");
					return new ResponseToProcess(HttpURLConnection.HTTP_OK, errMap);
				} else {
					// create a new status
					Map<String, SMValue> statusMap = new HashMap<String, SMValue>();
					statusMap.put("sm_owner", new SMString("user/" + username));
					statusMap.put("action", new SMString(""));
					statusMap.put("place", new SMString(""));
					statusMap.put("status_mod_date", new SMInt(System.currentTimeMillis()));
					SMObject statusObject = dataService.createObject("status", new SMObject(statusMap));
					SMString statusId = (SMString)statusObject.getValue().get("status_id");
					
					// add status as user's status
					List<SMString> statusIdList = new ArrayList<SMString>();
					statusIdList.add(statusId);
					dataService.addRelatedObjects("user", userId, "status", statusIdList);
					
					// add user as status's owner
					List<SMString> ownerIdList = new ArrayList<SMString>();
					ownerIdList.add(userId);
					dataService.addRelatedObjects("status", statusId, "owner", ownerIdList);
					
					// return created date for local database reference
					Map<String, Object> returnMap = new HashMap<String, Object>();
					SMInt statusModDate = (SMInt)statusObject.getValue().get("status_mod_date");
					returnMap.put("status_mod_date", statusModDate.getValue());
					return new ResponseToProcess(HttpURLConnection.HTTP_OK, returnMap);
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
