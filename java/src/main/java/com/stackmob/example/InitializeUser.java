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

public class InitializeUser implements CustomCodeMethod {

	@Override
	public String getMethodName() {
		return "initialize_user";
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
		if (username == null || username.isEmpty()) {
			HashMap<String, String> errParams = new HashMap<String, String>();
			errParams.put("error", "no user is logged in");
			return new ResponseToProcess(HttpURLConnection.HTTP_UNAUTHORIZED, errParams); // http 401 - unauthorized
		}
		SMString userId = new SMString(username);
		
		// get the datastore service
		DataService dataService = serviceProvider.getDataService();
		
		// create a response
		try {
			// fetch user object
			// - build query
			List<SMCondition> userQuery = new ArrayList<SMCondition>();
			userQuery.add(new SMEquals("username", userId));
			// - build result filter
			List<String> userFields = new ArrayList<String>();
			userFields.add("groups");
			ResultFilters userFilter = new ResultFilters(0, -1, null, userFields);
			// - execute query
			List<SMObject> users = dataService.readObjects("user", userQuery, 0, userFilter);
			// report error if query failed
			if (users == null || users.size() != 1) {
				HashMap<String, String> errMap = new HashMap<String, String>();
				errMap.put("error", "invalid user fetch");
				errMap.put("detail", (users == null ? "null fetch result" : ("fetch result count = " + users.size())));
				return new ResponseToProcess(HttpURLConnection.HTTP_INTERNAL_ERROR, errMap);
			}
			
			SMObject userObject = users.get(0);
			// report error if the user already have groups
			if (userObject.getValue().containsKey("groups")) {
				List<SMString> groups = ((SMList<SMString>)userObject.getValue().get("groups")).getValue();
				if (groups.size() > 0) {
					HashMap<String, String> errMap = new HashMap<String, String>();
					errMap.put("error", "user already has groups");
					return new ResponseToProcess(HttpURLConnection.HTTP_BAD_REQUEST, errMap); // http 400 - bad request
				}
			}
			
			// fetch relationship object
			// - build query
			List<SMCondition> relQuery = new ArrayList<SMCondition>();
			relQuery.add(new SMEquals("invite_email", userId));
			// - build result filter
			List<String> relFields = new ArrayList<String>();
			relFields.add("relationship_id");
			ResultFilters relFilter = new ResultFilters(0, -1, null, relFields);
			// - execute query
			List<SMObject> rels = dataService.readObjects("relationship", relQuery, 0, relFilter);
			// report error if query failed
			if (rels == null) {
				HashMap<String, String> errMap = new HashMap<String, String>();
				errMap.put("error", "invalid relationship fetch");
				errMap.put("detail", "null fetch result");
				return new ResponseToProcess(HttpURLConnection.HTTP_INTERNAL_ERROR, errMap);
			}
			
			Map<String, Object> returnMap = new HashMap<String, Object>();
			// 1. create initial groups
			List<String> titles = Arrays.asList("Favorites", "Close friends", "Family");
			List<SMString> groupIdList = new ArrayList<SMString>();
			for (int i = 0; i < titles.size(); i++) {
				String title = titles.get(i);
				// create a group
				Map<String, SMValue> groupMap = new HashMap<String, SMValue>();
				groupMap.put("sm_owner", new SMString("user/" + username));
				groupMap.put("title", new SMString(title));
				SMObject groupObject = dataService.createObject("group", new SMObject(groupMap));
				// get the group id
				SMString groupId = (SMString)groupObject.getValue().get("group_id");
				// store group id for adding later
				groupIdList.add(groupId);
				// add user as group's owner
				List<SMString> ownerIdList = new ArrayList<SMString>();
				ownerIdList.add(userId);
				dataService.addRelatedObjects("group", groupId, "owner", ownerIdList);
			}
			// add all groups in user's groups (and change group order)
			dataService.addRelatedObjects("user", userId, "groups", groupIdList);
			
			// update user's group order & groups mod date
			List<SMUpdate> userUpdates = new ArrayList<SMUpdate>();
			userUpdates.add(new SMSet("group_order", new SMList<SMString>(groupIdList)));
			returnMap.put("group_order", groupIdList);
			
			long currentTime = System.currentTimeMillis();
			userUpdates.add(new SMSet("groups_mod_date", new SMInt(currentTime)));
			dataService.updateObject("user", userId, userUpdates);
			
			// 2. connect invited relationships to user
			List<SMString> relIds = new ArrayList<SMString>();
			List<SMString> userIdList = new ArrayList<SMString>();
			userIdList.add(userId);
			for (int i = 0; i < rels.size(); i++) {
				SMObject relObject = rels.get(i);
				SMString relId = (SMString)relObject.getValue().get("relationship_id");
				// store relationship for adding later
				relIds.add(relId);
				// add user as relationship's receiver
				dataService.addRelatedObjects("relationship", relId, "receiver", userIdList);
				// empty invite email
				List<SMUpdate> relUpdates = new ArrayList<SMUpdate>();
				relUpdates.add(new SMSet("invite_email", new SMString("")));
				dataService.updateObject("relationship", relId, relUpdates);
			}
			// add all relationships in user's relationships_by_others
			dataService.addRelatedObjects("user", userId, "relationships_by_others", relIds);
			returnMap.put("relationship_ids", relIds);
			
			// return updated data for local database
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
