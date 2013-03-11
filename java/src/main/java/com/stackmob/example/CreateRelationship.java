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

public class CreateRelationship implements CustomCodeMethod {

	@Override
	public String getMethodName() {
		return "create_relationship";
	}
	
	@Override
	public List<String> getParams() {
		return Arrays.asList("username");
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
		
		// get requested friend's username
		String friendUsername = "";
		if (!request.getBody().isEmpty()) {
			try {
				JSONObject jsonObj = new JSONObject(request.getBody());
				if (!jsonObj.isNull("username")) {
					friendUsername = jsonObj.getString("username");
				}
			} catch (JSONException e) {
				HashMap<String, String> errParams = new HashMap<String, String>();
				errParams.put("error", "invalid request body");
				return new ResponseToProcess(HttpURLConnection.HTTP_BAD_REQUEST, errParams); // http 400 - bad request
			}
		}
		if (friendUsername == null || friendUsername.isEmpty()) {
			HashMap<String, String> errParams = new HashMap<String, String>();
			errParams.put("error", "username parameter not found");
			return new ResponseToProcess(HttpURLConnection.HTTP_BAD_REQUEST, errParams); // http 400 - bad request
		} else if (friendUsername.equals(username)) {
			HashMap<String, String> errParams = new HashMap<String, String>();
			errParams.put("error", "cannot add relationship with yourself");
			return new ResponseToProcess(HttpURLConnection.HTTP_BAD_REQUEST, errParams); // http 400 - bad request
		}
		SMString friendId = new SMString(friendUsername);
		
		// get the datastore service
		DataService dataService = serviceProvider.getDataService();
		
		// create a response
		try {
			// TO DO:
			// check if the relationship already exists first
			
			// create a new relationship
			Map<String, SMValue> relMap = new HashMap<String, SMValue>();
			relMap.put("sm_owner", new SMString("user/" + username));
			relMap.put("type_by_owner", new SMInt(2L));
			relMap.put("type_by_receiver", new SMInt(1L));
			SMObject relObject = dataService.createObject("relationship", new SMObject(relMap));
			SMString relId = (SMString)relObject.getValue().get("relationship_id");
			
			// add relationship in user's relationships_by_user
			List<SMString> relIdList = new ArrayList<SMString>();
			relIdList.add(relId);
			dataService.addRelatedObjects("user", userId, "relationships_by_user", relIdList);
			
			// add user as relationship's owner
			List<SMString> ownerIdList = new ArrayList<SMString>();
			ownerIdList.add(userId);
			dataService.addRelatedObjects("relationship", relId, "owner", ownerIdList);
			
			// add relationship in friend's relationships_by_others
			dataService.addRelatedObjects("user", friendId, "relationships_by_others", relIdList);
			
			// add friend as relationship's receiver
			List<SMString> receiverIdList = new ArrayList<SMString>();
			receiverIdList.add(friendId);
			dataService.addRelatedObjects("relationship", relId, "receiver", receiverIdList);
			
			// return created relationship data for local database
			Map<String, Object> returnMap = new HashMap<String, Object>();
			returnMap.put("relationship_id", relId.getValue());
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
