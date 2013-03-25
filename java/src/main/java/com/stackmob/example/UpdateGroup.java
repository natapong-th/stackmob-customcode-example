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
		return Arrays.asList("group_id", "title", "relationship_order", "block_ids", "delete_ids");
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
		List<SMString> blockIds = new ArrayList<SMString>();
		List<SMString> deleteIds = new ArrayList<SMString>();
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
			List<String> groupFields = new ArrayList<String>();
			groupFields.add("title");
			groupFields.add("relationship_order");
			groupFields.add("owner");
			groupFields.add("owner.username");
			groupFields.add("owner.relationships_by_user");
			groupFields.add("owner.relationships_by_others");
			groupFields.add("relationships_by_owner");
			groupFields.add("relationships_by_owner.relationship_id");
			groupFields.add("relationships_by_others");
			groupFields.add("relationships_by_others.relationship_id");
			ResultFilters groupFilter = new ResultFilters(0, -1, null, groupFields);
			// - execute query
			List<SMObject> groups = dataService.readObjects("group", groupQuery, 1, groupFilter);
			// report error if query failed
			if (groups == null || groups.size() != 1) {
				HashMap<String, String> errMap = new HashMap<String, String>();
				errMap.put("error", "invalid group fetch");
				errMap.put("detail", (groups == null ? "null fetch result" : ("fetch result count = " + groups.size())));
				return new ResponseToProcess(HttpURLConnection.HTTP_INTERNAL_ERROR, errMap);
			}
			
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
					if (relOrder.remove(relId)) {
						foundList.add(relId);
					} else {
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
					if (relOrder.remove(relId)) {
						foundList.add(relId);
					} else {
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
				// 2.3. change relationship order
				groupUpdates.add(new SMSet("relationship_order", new SMList<SMString>(newRelOrder)));
				returnMap.put("relationship_order", newRelOrder);
			}
			// update the group
			dataService.updateObject("group", groupId, groupUpdates);
			
			// 3. change groups mod date
			long currentTime = System.currentTimeMillis();
			List<SMUpdate> userUpdates = new ArrayList<SMUpdate>();
			userUpdates.add(new SMSet("groups_mod_date", new SMInt(currentTime)));
			dataService.updateObject("user", userId, userUpdates);
			
			// 4. block and delete input ids (if any)
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
					SMString relOwnerId = (SMString)relObject.getValue().get("owner");
					String userRole = "";
					if (relOwnerId.equals(userId)) {
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
									List<SMString> groupIdList = new ArrayList<SMString>();
									for (int j = 0; j < groupsList.size(); j++) {
										SMObject relGroupObject = groupsList.get(j);
										SMString relGroupId = (SMString)relGroupObject.getValue().get("group_id");
										dataService.removeRelatedObjects("group", relGroupId, relKey, relIdList, false);
										groupIdList.add(relGroupId);
										// remove from group's relationship order as well
										SMList<SMString> relOrderValue = (SMList<SMString>)relGroupObject.getValue().get("relationship_order");
										List<SMString> groupRelOrder = relOrderValue.getValue();
										if (groupRelOrder.remove(relId)) {
											List<SMUpdate> relGroupUpdates = new ArrayList<SMUpdate>();
											relGroupUpdates.add(new SMSet("relationship_order", new SMList<SMString>(groupRelOrder)));
											dataService.updateObject("group", relGroupId, relGroupUpdates);
										}
									}
									// remove groups from relationship's groups by user
									dataService.removeRelatedObjects("relationship", relId, groupKey, groupIdList, false);
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
