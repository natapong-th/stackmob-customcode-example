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

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class UpdateGroup implements CustomCodeMethod {

	@Override
	public String getMethodName() {
		return "update_group";
	}
	
	@Override
	public List<String> getParams() {
		return Arrays.asList("group_id", "title", "relationship_order");
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
		String groupId = "";
		String title = "";
		boolean newTitle = false;
		String relOrder = "";
		boolean newOrder = false;
		if (!request.getBody().isEmpty()) {
			try {
				JSONObject jsonObj = new JSONObject(request.getBody());
				if (!jsonObj.isNull("group_id")) {
					groupId = jsonObj.getString("group_id");
				}
				if (!jsonObj.isNull("title")) {
					title = jsonObj.getString("title");
					newTitle = true;
				}
				if (!jsonObj.isNull("relationship_order")) {
					relOrder = jsonObj.getString("relationship_order");
					newOrder = true;
				}
			} catch (JSONException e) {
				HashMap<String, String> errParams = new HashMap<String, String>();
				errParams.put("error", "invalid request body");
				return new ResponseToProcess(HttpURLConnection.HTTP_BAD_REQUEST, errParams); // http 400 - bad request
			}
		}
		if (groupId.isEmpty() || (!newTitle && !newOrder)) {
			HashMap<String, String> errParams = new HashMap<String, String>();
			errParams.put("error", "invalid parameters");
			return new ResponseToProcess(HttpURLConnection.HTTP_BAD_REQUEST, errParams); // http 400 - bad request
		}
		
		// get the datastore service
		DataService dataService = serviceProvider.getDataService();
		
		// create a response
		try {
			// fetch group object
			// - build query
			List<SMCondition> groupQuery = new ArrayList<SMCondition>();
			groupQuery.add(new SMEquals("group_id", new SMString(groupId)));
			// - build result filter
			List<String> fields = new ArrayList<String>();
			fields.add("title");
			fields.add("relationship_order");
			fields.add("owner");
			fields.add("owner.username");
			fields.add("owner.relationships_by_user");
			fields.add("owner.relationships_by_others");
			fields.add("relationships_by_owner");
			fields.add("relationships_by_owner.relationship_id");
			fields.add("relationships_by_others");
			fields.add("relationships_by_others.relationship_id");
			ResultFilters filter = new ResultFilters(0, -1, null, fields);
			// - execute query
			List<SMObject> groups = dataService.readObjects("group", groupQuery, 1, filter);
			if (groups != null && groups.size() == 1) {
				SMObject groupObject = groups.get(i);
				// check if this user is the owner
				SMObject ownerObject = (SMObject)groupObject.getValue().get("owner");
				SMString ownerUsername = (SMString)ownerObject.getValue().get("username");
				if (!ownerUsername.getValue().equals(username)) {
					HashMap<String, String> errParams = new HashMap<String, String>();
					errParams.put("error", "requested groups are inaccessible by this user");
					return new ResponseToProcess(HttpURLConnection.HTTP_BAD_REQUEST, errParams); // http 400 - bad request
				}
				Map<String, Object> returnMap = new HashMap<String, Object>();
				List<SMUpdate> groupUpdates = new ArrayList<SMUpdate>();
				// 1. change title
				if (newTitle) {
					groupUpdates.add(new SMSet("title", new SMString(title)));
					returnMap.put("title", title);
				}
				// 2. change relationships
				if (newOrder) {
					String foundRelIds = "";
					String removedRelIds = "";
					String addedRelIds = "";
					// 2.1 remove all relationships that are not in relationship order
					// relationships by owner
					List<SMString> relsOwner = new ArrayList<SMString>();
					if (groupObject.getValue().containsKey("relationships_by_owner")) {
						relsOwner = ((SMList<SMObject>)groupObject.getValue().get("relationships_by_owner")).getValue();
					}
					List<SMString> removeList = new ArrayList<SMString>();
					for (int i = 0; i < relsOwner.size(); i++) {
						SMObject relObject = relsOwner.get(i);
						SMString relId = relObject.getValue().get("relationship_id");
						if (relOrder.indexOf(relId.getValue()) != -1) {
							foundRelIds = foundRelIds + relId.getValue() + "|";
						} else {
							removeList.add(relId);
							removedRelIds = removedRelIds + relId.getValue() + "|";
						}
					}
					dataService.removeRelatedObjects("group", new SMString(groupId), "relationships_by_owner", removeList, false);
					// relationships by others
					List<SMObject> relsOthers = new ArrayList<SMString>();
					if (groupObject.getValue().containsKey("relationships_by_owner")) {
						relsOthers = ((SMList<SMObject>)groupObject.getValue().get("relationships_by_others")).getValue();
					}
					removeList = new ArrayList<SMString>();
					for (int i = 0; i < relsOthers.size(); i++) {
						SMObject relObject = relsOthers.get(i);
						SMString relId = relObject.getValue().get("relationship_id");
						if (relOrder.indexOf(relId.getValue()) != -1) {
							foundRelIds = foundRelIds + relId.getValue() + "|";
						} else {
							removeList.add(relId);
							removedRelIds = removedRelIds + relId.getValue() + "|";
						}
					}
					dataService.removeRelatedObjects("group", new SMString(groupId), "relationships_by_others", removeList, false);
					returnMap.put("unchanged_relationships", foundRelIds);
					returnMap.put("removed_relationships", removedRelIds);
					// 2.2 add all relationships that are in relationship order but not already added
					// all relationships by user
					List<SMString> allRelsUser = new ArrayList<SMString>();
					if (ownerObject.getValue().containsKey("relationships_by_user")) {
						allRelsUser = ((SMList<SMString>)ownerObject.getValue().get("relationships_by_user")).getValue();
					}
					List<SMString> addList = new ArrayList<SMString>();
					for (int i = 0; i < allRelsUser.size(); i++) {
						SMString relId = allRelsUser.get(i);
						if (foundRelIds.indexOf(relId.getValue()) == -1 && relOrder.indexOf(relId.getValue()) != -1) {
							addList.add(relId);
							addedRelIds = addedRelIds + relId.getValue() + "|";
						}
					}
					dataService.addRelatedObjects("group", new SMString(groupId), "relationships_by_owner", addList);
					// all relationships by others
					List<SMString> allRelsOthers = new ArrayList<SMString>();
					if (ownerObject.getValue().containsKey("relationships_by_others")) {
						allRelsOthers = ((SMList<SMString>)ownerObject.getValue().get("relationships_by_others")).getValue();
					}
					addList = new ArrayList<SMString>();
					for (int i = 0; i < allRelsOthers.size(); i++) {
						SMString relId = allRelsOthers.get(i);
						if (foundRelIds.indexOf(relId.getValue()) == -1 && relOrder.indexOf(relId.getValue()) != -1) {
							addList.add(relId);
							addedRelIds = addedRelIds + relId.getValue() + "|";
						}
					}
					dataService.addRelatedObjects("group", new SMString(groupId), "relationships_by_others", addList);
					returnMap.put("added_relationships", addedRelIds);
					// 3. change relationship order
					groupUpdates.add(new SMSet("relationship_order", new SMString(relOrder)));
					// update the group
					dataService.updateObject("group", new SMString(groupId), relUpdates);
					returnMap.put("relationship_order", relOrder);
				}
				// return updated data for local database
				return new ResponseToProcess(HttpURLConnection.HTTP_OK, returnMap);
			} else {
				// TO DO:
				// handle group fetch error
				
				HashMap<String, String> errMap = new HashMap<String, String>();
				errMap.put("error", "invalid group fetch");
				errMap.put("detail", (groups == null ? "null fetch result" : ("fetch result count = " + groups.size())));
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
