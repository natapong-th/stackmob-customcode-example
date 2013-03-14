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
		if (username == null || username.isEmpty()) {
			HashMap<String, String> errParams = new HashMap<String, String>();
			errParams.put("error", "no user is logged in");
			return new ResponseToProcess(HttpURLConnection.HTTP_UNAUTHORIZED, errParams); // http 401 - unauthorized
		}
		SMString userId = new SMString(username);
		
		// get update parameters
		String groupIdString = "";
		String title = "";
		boolean newTitle = false;
		List<SMString> relOrder = new ArrayList<SMString>();
		boolean newOrder = false;
		if (!request.getBody().isEmpty()) {
			try {
				JSONObject jsonObj = new JSONObject(request.getBody());
				if (!jsonObj.isNull("group_id")) {
					groupIdString = jsonObj.getString("group_id");
				}
				if (!jsonObj.isNull("title")) {
					title = jsonObj.getString("title");
					newTitle = true;
				}
				if (!jsonObj.isNull("relationship_order")) {
					JSONArray relArray = jsonObj.getJSONArray("relationship_order");
					for (int i = 0; i < relArray.length(); i++) {
						String relId = relArray.getString(i);
						relOrder.add(new SMString(relId));
					}
					newOrder = true;
				}
			} catch (JSONException e) {
				HashMap<String, String> errParams = new HashMap<String, String>();
				errParams.put("error", "invalid request body");
				return new ResponseToProcess(HttpURLConnection.HTTP_BAD_REQUEST, errParams); // http 400 - bad request
			}
		}
		if (groupIdString.isEmpty() || (!newTitle && !newOrder)) {
			HashMap<String, String> errParams = new HashMap<String, String>();
			errParams.put("error", "invalid parameters");
			return new ResponseToProcess(HttpURLConnection.HTTP_BAD_REQUEST, errParams); // http 400 - bad request
		}
		SMString groupId = new SMString(groupIdString);
		
		// get the datastore service
		DataService dataService = serviceProvider.getDataService();
		
		// create a response
		try {
			// fetch group object
			// - build query
			List<SMCondition> groupQuery = new ArrayList<SMCondition>();
			groupQuery.add(new SMEquals("group_id", groupId));
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
				SMObject groupObject = groups.get(0);
				// check if this user is the owner
				SMObject ownerObject = (SMObject)groupObject.getValue().get("owner");
				SMString ownerId = (SMString)ownerObject.getValue().get("username");
				if (!ownerId.equals(userId)) {
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
					List<SMString> foundList = new ArrayList<SMString>();
					List<SMString> removeList = new ArrayList<SMString>();
					List<SMString> addList = new ArrayList<SMString>();
					List<SMString> newRelOrder = new ArrayList<SMString>(relOrder);
					List<SMString> groupIdList = new ArrayList<SMString>();
					groupIdList.add(groupId);
					// 2.1. remove all relationships that are not in relationship order
					// - relationships by owner
					List<SMObject> relsOwner = new ArrayList<SMObject>();
					if (groupObject.getValue().containsKey("relationships_by_owner")) {
						relsOwner = ((SMList<SMObject>)groupObject.getValue().get("relationships_by_owner")).getValue();
					}
					List<SMString> tempRemoveList = new ArrayList<SMString>();
					for (int i = 0; i < relsOwner.size(); i++) {
						SMObject relObject = relsOwner.get(i);
						SMString relId = (SMString)relObject.getValue().get("relationship_id");
						boolean found = false;
						for (int j = 0; j < relOrder.size(); j++) {
							if (relOrder.get(j).equals(relId)) {
								found = true;
								foundList.add(relId);
								relOrder.remove(j);
								break;
							}
						}
						if (!found) {
							dataService.removeRelatedObjects("relationship", relId, "groups_by_owner", groupIdList, false);
							tempRemoveList.add(relId);
							removeList.add(relId);
						}
					}
					dataService.removeRelatedObjects("group", groupId, "relationships_by_owner", tempRemoveList, false);
					// - relationships by others
					List<SMObject> relsOthers = new ArrayList<SMObject>();
					if (groupObject.getValue().containsKey("relationships_by_others")) {
						relsOthers = ((SMList<SMObject>)groupObject.getValue().get("relationships_by_others")).getValue();
					}
					tempRemoveList = new ArrayList<SMString>();
					for (int i = 0; i < relsOthers.size(); i++) {
						SMObject relObject = relsOthers.get(i);
						SMString relId = (SMString) relObject.getValue().get("relationship_id");
						boolean found = false;
						for (int j = 0; j < relOrder.size(); j++) {
							if (relOrder.get(j).equals(relId)) {
								found = true;
								foundList.add(relId);
								relOrder.remove(j);
								break;
							}
						}
						if (!found) {
							dataService.removeRelatedObjects("relationship", relId, "groups_by_receiver", groupIdList, false);
							tempRemoveList.add(relId);
							removeList.add(relId);
						}
					}
					dataService.removeRelatedObjects("group", groupId, "relationships_by_others", tempRemoveList, false);
					returnMap.put("unchanged_relationships", foundList);
					returnMap.put("removed_relationships", removeList);
					// 2.2. add relationships left in relationship order
					//     and remove non-existing relationship from the order
					// - all relationships by user
					List<SMString> allRelsUser = new ArrayList<SMString>();
					if (ownerObject.getValue().containsKey("relationships_by_user")) {
						allRelsUser = ((SMList<SMString>)ownerObject.getValue().get("relationships_by_user")).getValue();
					}
					List<SMString> userAddList = new ArrayList<SMString>();
					// - all relationships by others
					List<SMString> allRelsOthers = new ArrayList<SMString>();
					if (ownerObject.getValue().containsKey("relationships_by_others")) {
						allRelsOthers = ((SMList<SMString>)ownerObject.getValue().get("relationships_by_others")).getValue();
					}
					List<SMString> othersAddList = new ArrayList<SMString>();
					for (int i = 0; i < relOrder.size(); i++) {
						boolean found = false;
						SMString relId = relOrder.get(i);
						for (int j = 0; j < allRelsUser.size(); j++) {
							if (relId.equals(allRelsUser.get(j))) {
								found = true;
								break;
							}
						}
						if (found) {
							dataService.addRelatedObjects("relationship", relId, "groups_by_owner", groupIdList);
							userAddList.add(relId);
							addList.add(relId);
						} else {
							for (int j = 0; j < allRelsOthers.size(); j++) {
								if (relId.equals(allRelsOthers.get(j))) {
									found = true;
									break;
								}
							}
							if (found) {
								dataService.addRelatedObjects("relationship", relId, "groups_by_receiver", groupIdList);
								othersAddList.add(relId);
								addList.add(relId);
							} else {
								newRelOrder.remove(relId);
							}
						}
					}
					dataService.addRelatedObjects("group", groupId, "relationships_by_owner", userAddList);
					dataService.addRelatedObjects("group", groupId, "relationships_by_others", othersAddList);
					returnMap.put("added_relationships", addList);
					// 2.3. change relationship order
					returnMap.put("relationship_order", newRelOrder);
					groupUpdates.add(new SMSet("relationship_order", new SMList<SMString>(newRelOrder)));
				}
				// update the group
				dataService.updateObject("group", groupId, groupUpdates);
				returnMap.put("relationship_order", relOrder);
				// 4. change groups mod date
				long currentTime = System.currentTimeMillis();
				List<SMUpdate> userUpdates = new ArrayList<SMUpdate>();
				userUpdates.add(new SMSet("groups_mod_date", new SMInt(currentTime)));
				dataService.updateObject("user", userId, userUpdates);
				returnMap.put("groups_mod_date", new Long(currentTime));
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
