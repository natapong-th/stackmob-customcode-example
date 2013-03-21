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

public class CreateNewGroup implements CustomCodeMethod {

	@Override
	public String getMethodName() {
		return "create_new_group";
	}
	
	@Override
	public List<String> getParams() {
		return Arrays.asList("title", "relationship_order", "block_ids", "delete_ids");
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
		String title = "";
		List<SMString> relOrder = new ArrayList<SMString>();
		List<SMString> blockIds = new ArrayList<SMString>();
		List<SMString> deleteIds = new ArrayList<SMString>();
		if (!request.getBody().isEmpty()) {
			try {
				JSONObject jsonObj = new JSONObject(request.getBody());
				if (!jsonObj.isNull("title")) {
					title = jsonObj.getString("title");
				}
				if (!jsonObj.isNull("block_ids")) {
					JSONArray relArray = jsonObj.getJSONArray("relationship_order");
					for (int i = 0; i < relArray.length(); i++) {
						String relId = relArray.getString(i);
						relOrder.add(new SMString(relId));
					}
				}
				if (!jsonObj.isNull("block_ids")) {
					JSONArray relArray = jsonObj.getJSONArray("block_ids");
					for (int i = 0; i < relArray.length(); i++) {
						String relId = relArray.getString(i);
						blockIds.add(new SMString(relId));
					}
				}
				if (!jsonObj.isNull("delete_ids")) {
					JSONArray relArray = jsonObj.getJSONArray("delete_ids");
					for (int i = 0; i < relArray.length(); i++) {
						String relId = relArray.getString(i);
						deleteIds.add(new SMString(relId));
					}
				}
			} catch (JSONException e) {
				HashMap<String, String> errParams = new HashMap<String, String>();
				errParams.put("error", "invalid request body");
				return new ResponseToProcess(HttpURLConnection.HTTP_BAD_REQUEST, errParams); // http 400 - bad request
			}
		}
		if (title.isEmpty()) {
			HashMap<String, String> errParams = new HashMap<String, String>();
			errParams.put("error", "invalid parameters");
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
			List<String> userFields = new ArrayList<String>();
			userFields.add("group_order");
			userFields.add("relationships_by_user");
			userFields.add("relationships_by_others");
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
			
			Map<String, Object> returnMap = new HashMap<String, Object>();
			SMObject userObject = users.get(0);
			// 1. create a new group
			Map<String, SMValue> groupMap = new HashMap<String, SMValue>();
			groupMap.put("sm_owner", new SMString("user/" + username));
			groupMap.put("title", new SMString(title));
			SMObject groupObject = dataService.createObject("group", new SMObject(groupMap));
			// get the new group id
			SMString groupId = (SMString)groupObject.getValue().get("group_id");
			// add group in user's groups
			List<SMString> groupIdList = new ArrayList<SMString>();
			groupIdList.add(groupId);
			dataService.addRelatedObjects("user", userId, "groups", groupIdList);
			// add user as group's owner
			List<SMString> ownerIdList = new ArrayList<SMString>();
			ownerIdList.add(userId);
			dataService.addRelatedObjects("group", groupId, "owner", ownerIdList);
			
			returnMap.put("group_id", groupId);
			
			// 2. add relationships in relationship order to the group
			// and remove non-existing relationship from the order
			if (relOrder.size() > 0) {
				List<SMString> addList = new ArrayList<SMString>();
				List<SMString> newRelOrder = new ArrayList<SMString>(relOrder);
				// - all relationships by user
				List<SMString> allRelsUser = new ArrayList<SMString>();
				if (userObject.getValue().containsKey("relationships_by_user")) {
					allRelsUser = ((SMList<SMString>)userObject.getValue().get("relationships_by_user")).getValue();
				}
				List<SMString> userAddList = new ArrayList<SMString>();
				// - all relationships by others
				List<SMString> allRelsOthers = new ArrayList<SMString>();
				if (userObject.getValue().containsKey("relationships_by_others")) {
					allRelsOthers = ((SMList<SMString>)userObject.getValue().get("relationships_by_others")).getValue();
				}
				List<SMString> othersAddList = new ArrayList<SMString>();
				for (int i = 0; i < relOrder.size(); i++) {
					SMString relId = relOrder.get(i);
					if (allRelsUser.contains(relId)) {
						dataService.addRelatedObjects("relationship", relId, "groups_by_owner", groupIdList);
						userAddList.add(relId);
						addList.add(relId);
					} else if (allRelsOthers.contains(relId)) {
						dataService.addRelatedObjects("relationship", relId, "groups_by_receiver", groupIdList);
						othersAddList.add(relId);
						addList.add(relId);
					} else {
						newRelOrder.remove(relId);
					}
				}
				dataService.addRelatedObjects("group", groupId, "relationships_by_owner", userAddList);
				dataService.addRelatedObjects("group", groupId, "relationships_by_others", othersAddList);
				returnMap.put("added_relationships", addList);
				
				// update relationship order
				List<SMUpdate> groupUpdates = new ArrayList<SMUpdate>();
				groupUpdates.add(new SMSet("relationship_order", new SMList<SMString>(newRelOrder)));
				dataService.updateObject("group", groupId, groupUpdates);
				returnMap.put("relationship_order", newRelOrder);
			}
			
			// 3. update user's group order & groups mod date
			List<SMString> groupOrder = new ArrayList<SMString>();
			if (userObject.getValue().containsKey("group_order")) {
				groupOrder = ((SMList<SMString>)userObject.getValue().get("group_order")).getValue();
			}
			groupOrder.add(groupId);
			List<SMUpdate> userUpdates = new ArrayList<SMUpdate>();
			userUpdates.add(new SMSet("group_order", new SMList<SMString>(groupOrder)));
			returnMap.put("group_order", groupId);
			
			long currentTime = System.currentTimeMillis();
			userUpdates.add(new SMSet("groups_mod_date", new SMInt(currentTime)));
			dataService.updateObject("user", userId, userUpdates);
			
			// 3. block and delete input ids (if any)
			if (blockIds.size() + deleteIds.size() > 0) {
				// fetch relationship objects
				// - build query
				List<SMString> allIds = new ArrayList<SMString>(blockIds);
				allIds.addAll(deleteIds);
				List<SMCondition> relQuery = new ArrayList<SMCondition>();
				relQuery.add(new SMIn("relationship_id", allIds));
				// - build result filter
				List<String> relFields = new ArrayList<String>();
				relFields.add("relationship_id");
				relFields.add("type_by_owner");
				relFields.add("type_by_receiver");
				relFields.add("owner");
				relFields.add("receiver");
				relFields.add("events_by_owner");
				relFields.add("events_by_receiver");
				relFields.add("groups_by_owner");
				relFields.add("groups_by_owner.group_id");
				relFields.add("groups_by_owner.relationship_order");
				relFields.add("groups_by_receiver");
				relFields.add("groups_by_receiver.group_id");
				relFields.add("groups_by_receiver.relationship_order");
				ResultFilters relFilter = new ResultFilters(0, -1, null, relFields);
				// - execute query
				List<SMObject> rels = dataService.readObjects("relationship", relQuery, 1, relFilter);
				// report error & return partial result if query failed
				if (rels == null || rels.size() != allIds.size()) {
					returnMap.put("error", allIds);
					return new ResponseToProcess(HttpURLConnection.HTTP_OK, returnMap);
				}
				
				List<SMString> foundRelIds = new ArrayList<SMString>();
				List<SMString> removedEventIds = new ArrayList<SMString>();
				for (int i = 0; i < rels.size(); i++) {
					SMObject relObject = rels.get(i);
					SMString relId = (SMString)relObject.getValue().get("relationship_id");
					// find user's role in this relationship
					SMString ownerId = (SMString)relObject.getValue().get("owner");
					String userRole = "";
					if (ownerId.equals(userId)) {
						userRole = "owner";
					} else if (relObject.getValue().containsKey("receiver")) {
						SMString receiverId = (SMString)relObject.getValue().get("receiver");
						if (receiverId.equals(userId)) {
							userRole = "receiver";
						}
					}
					// if user is in this relationship, change its type by user
					if (!userRole.isEmpty()) {
						long type = blockIds.contains(relId) ? 3L : 4L;
						String typeUserKey = "type_by_" + userRole;
						SMInt typeUser = (SMInt)relObject.getValue().get(typeUserKey);
						if (typeUser.getValue().longValue() != type) {
							String typeOtherKey = "type_by_" + (userRole.equals("owner") ? "receiver" : "owner");
							SMInt typeOther = (SMInt)relObject.getValue().get(typeOtherKey);
							// remove all events from both sides (no need to remove if any of the types is already block or delete)
							if (typeUser.getValue().longValue() < 3L && typeOther.getValue().longValue() < 3L) {
								if (relObject.getValue().containsKey("events_by_owner")) {
									SMList<SMString> events = (SMList<SMString>)relObject.getValue().get("events_by_owner");
									dataService.removeRelatedObjects("relationship", relId, "events_by_owner", events, true);
									if (userRole.equals("receiver")) {
										removedEventIds.addAll(events.getValue());
									}
								}
								if (relObject.getValue().containsKey("events_by_receiver")) {
									SMList<SMString> events = (SMList<SMString>)relObject.getValue().get("events_by_receiver");
									dataService.removeRelatedObjects("relationship", relId, "events_by_receiver", events, true);
									if (userRole.equals("owner")) {
										removedEventIds.addAll(events.getValue());
									}
								}
							}
							// if type changes from friend, remove this relationship from all groups
							if (typeUser.getValue().longValue() < 3L) {
								String groupKey = "groups_by_" + userRole;
								if (relObject.getValue().containsKey(groupKey)) {
									SMList<SMObject> groupsValue = (SMList<SMObject>)relObject.getValue().get(groupKey);
									List<SMObject> groupsList = groupsValue.getValue();
									// remove relationship from each group's relationships by user
									List<SMString> relIdList = new ArrayList<SMString>();
									relIdList.add(relId);
									String relKey = "relationships_by_" + (userRole.equals("owner") ? "owner" : "others");
									List<SMString> relGroupIdList = new ArrayList<SMString>();
									for (int j = 0; j < groupsList.size(); j++) {
										SMObject relGroupObject = groupsList.get(j);
										SMString relGroupId = (SMString)relGroupObject.getValue().get("group_id");
										dataService.removeRelatedObjects("group", relGroupId, relKey, relIdList, false);
										relGroupIdList.add(relGroupId);
										// remove from group's relationship order as well
										SMList<SMString> relOrderValue = (SMList<SMString>)relGroupObject.getValue().get("relationship_order");
										List<SMString> groupRelOrder = relOrderValue.getValue();
										if (groupRelOrder.remove(relId)) {
											List<SMUpdate> groupUpdates = new ArrayList<SMUpdate>();
											groupUpdates.add(new SMSet("relationship_order", new SMList<SMString>(groupRelOrder)));
											dataService.updateObject("group", relGroupId, groupUpdates);
										}
									}
									// remove groups from relationship's groups by user
									dataService.removeRelatedObjects("relationship", relId, groupKey, relGroupIdList, false);
								}
							}
							// update type by user
							List<SMUpdate> relUpdates = new ArrayList<SMUpdate>();
							relUpdates.add(new SMSet(typeUserKey, new SMInt(type)));
							dataService.updateObject("relationship", relId, relUpdates);
							
							foundRelIds.add(relId);
						}
					}
				}
				returnMap.put("changed_relationships", foundRelIds);
				returnMap.put("removed_events", removedEventIds);
			}
			
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
