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

public class UpdateRelationships implements CustomCodeMethod {

	@Override
	public String getMethodName() {
		return "update_relationships";
	}
	
	@Override
	public List<String> getParams() {
		return Arrays.asList("accept_ids", "block_ids", "delete_ids");
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
		List<SMString> acceptIds = new ArrayList<SMString>();
		List<SMString> blockIds = new ArrayList<SMString>();
		List<SMString> deleteIds = new ArrayList<SMString>();
		if (!request.getBody().isEmpty()) {
			try {
				JSONObject jsonObj = new JSONObject(request.getBody());
				if (!jsonObj.isNull("accept_ids")) {
					JSONArray relIdArray = jsonObj.getJSONArray("accept_ids");
					for (int i = 0; i < relIdArray.length(); i++) {
						String relId = relIdArray.getString(i);
						acceptIds.add(new SMString(relId));
					}
				}
				if (!jsonObj.isNull("block_ids")) {
					JSONArray relIdArray = jsonObj.getJSONArray("block_ids");
					for (int i = 0; i < relIdArray.length(); i++) {
						String relId = relIdArray.getString(i);
						blockIds.add(new SMString(relId));
					}
				}
				if (!jsonObj.isNull("delete_ids")) {
					JSONArray relIdArray = jsonObj.getJSONArray("delete_ids");
					for (int i = 0; i < relIdArray.length(); i++) {
						String relId = relIdArray.getString(i);
						deleteIds.add(new SMString(relId));
					}
				}
			} catch (Exception e) {
				HashMap<String, String> errParams = new HashMap<String, String>();
				errParams.put("error", "invalid request body");
				return new ResponseToProcess(HttpURLConnection.HTTP_BAD_REQUEST, errParams); // http 400 - bad request
			}
		}
		if (acceptIds.size() + blockIds.size() + deleteIds.size() == 0) {
			HashMap<String, String> errParams = new HashMap<String, String>();
			errParams.put("error", "invalid parameters");
			return new ResponseToProcess(HttpURLConnection.HTTP_BAD_REQUEST, errParams); // http 400 - bad request
		}
		
		// get the datastore service
		DataService dataService = serviceProvider.getDataService();
		
		// create a response
		try {
			// fetch relationship objects
			// - build query
			List<SMString> allIds = new ArrayList<SMString>(acceptIds);
			allIds.addAll(blockIds);
			allIds.addAll(deleteIds);
			List<SMCondition> relQuery = new ArrayList<SMCondition>();
			relQuery.add(new SMIn("relationship_id", allIds));
			// - build result filter
			List<String> fields = new ArrayList<String>();
			fields.add("relationship_id");
			fields.add("type_by_owner");
			fields.add("type_by_receiver");
			fields.add("invite_email");
			fields.add("owner");
			fields.add("owner.username");
			fields.add("owner.action");
			fields.add("owner.place");
			fields.add("owner.status_mod_date");
			fields.add("receiver");
			fields.add("receiver.username");
			fields.add("receiver.action");
			fields.add("receiver.place");
			fields.add("receiver.status_mod_date");
			fields.add("events_by_owner");
			fields.add("events_by_receiver");
			fields.add("groups_by_owner");
			fields.add("groups_by_owner.group_id");
			fields.add("groups_by_owner.relationship_order");
			fields.add("groups_by_receiver");
			fields.add("groups_by_receiver.group_id");
			fields.add("groups_by_receiver.relationship_order");
			ResultFilters filter = new ResultFilters(0, -1, null, fields);
			// - execute query
			List<SMObject> rels = dataService.readObjects("relationship", relQuery, 1, filter);
			// report error if query failed
			if (rels == null || rels.size() != allIds.size()) {
				HashMap<String, String> errMap = new HashMap<String, String>();
				errMap.put("error", "invalid relationship fetch");
				errMap.put("detail", (rels == null ? "null fetch result" : ("fetch result count = " + rels.size())));
				return new ResponseToProcess(HttpURLConnection.HTTP_INTERNAL_ERROR, errMap);
			}
			
			Map<String, Object> returnMap = new HashMap<String, Object>();
			List<Map<String, Object>> foundFriends = new ArrayList<Map<String, Object>>();
			List<SMString> foundRelIds = new ArrayList<SMString>();
			List<SMString> removedEventIds = new ArrayList<SMString>();
			boolean groupChange = false;
			for (int i = 0; i < rels.size(); i++) {
				SMObject relObject = rels.get(i);
				SMString relId = (SMString)relObject.getValue().get("relationship_id");
				// find user's role in this relationship
				SMObject ownerObject = (SMObject)relObject.getValue().get("owner");
				SMString ownerId = (SMString)ownerObject.getValue().get("username");
				String userRole = "";
				if (ownerId.equals(userId)) {
					userRole = "owner";
				} else if (relObject.getValue().containsKey("receiver")) {
					SMObject receiverObject = (SMObject)relObject.getValue().get("receiver");
					SMString receiverId = (SMString)receiverObject.getValue().get("username");
					if (receiverId.equals(userId)) {
						userRole = "receiver";
					}
				}
				// if user is in this relationship, change its type by user
				if (!userRole.isEmpty()) {
					long type = 2L;
					if (blockIds.contains(relId)) {
						type = 3L;
					}
					else if (deleteIds.contains(relId)) {
						type = 4L;
					}
					String typeUserKey = "type_by_" + userRole;
					SMInt typeUser = (SMInt)relObject.getValue().get(typeUserKey);
					if (typeUser.getValue().longValue() != type) {
						// if type changes from no response to accepted, create a friend accept event (only if you're not blocked/deleted)
						String typeOtherKey = "type_by_" + (userRole.equals("owner") ? "receiver" : "owner");
						SMInt typeOther = (SMInt)relObject.getValue().get(typeOtherKey);
						if (type == 2L && typeUser.getValue().longValue() == 1L && typeOther.getValue().longValue() == 2L) {
							Map<String, SMValue> eventMap = new HashMap<String, SMValue>();
							eventMap.put("sm_owner", new SMString("user/" + username));
							eventMap.put("type", new SMInt(2L));
							SMObject eventObject = dataService.createObject("event", new SMObject(eventMap));
							// get the new event id
							SMString eventId = (SMString)eventObject.getValue().get("event_id");
							// add event in relationship's events_by_owner
							List<SMString> joinEventIdList = new ArrayList<SMString>();
							joinEventIdList.add(eventId);
							dataService.addRelatedObjects("relationship", relId, "events_by_" + userRole, joinEventIdList);
							// add relationship as event's relationship
							List<SMString> relIdList = new ArrayList<SMString>();
							relIdList.add(relId);
							dataService.addRelatedObjects("event", eventId, "relationship_by_" + userRole, relIdList);
						}
						// if type changes to block or delete, remove all events from both sides
						// no need to remove if any of the types is already block or delete
						if (type >= 3L && typeUser.getValue().longValue() < 3L && typeOther.getValue().longValue() < 3L) {
							if (relObject.getValue().containsKey("events_by_owner")) {
								SMList<SMString> eventsValue = (SMList<SMString>)relObject.getValue().get("events_by_owner");
								List<SMString> events = eventsValue.getValue();
								if (events.size() > 0) {
									dataService.removeRelatedObjects("relationship", relId, "events_by_owner", events, true);
								}
								if (userRole.equals("receiver")) {
									removedEventIds.addAll(events);
								}
							}
							if (relObject.getValue().containsKey("events_by_receiver")) {
								SMList<SMString> eventsValue = (SMList<SMString>)relObject.getValue().get("events_by_receiver");
								List<SMString> events = eventsValue.getValue();
								if (events.size() > 0) {
									dataService.removeRelatedObjects("relationship", relId, "events_by_receiver", events, true);
								}
								if (userRole.equals("owner")) {
									removedEventIds.addAll(events);
								}
							}
						}
						// if type changes from friend to block or delete, remove this relationship from all groups
						if (type >= 3L && typeUser.getValue().longValue() < 3L) {
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
									SMObject groupObject = groupsList.get(j);
									SMString groupId = (SMString)groupObject.getValue().get("group_id");
									dataService.removeRelatedObjects("group", groupId, relKey, relIdList, false);
									groupIdList.add(groupId);
									// remove from group's relationship order as well
									SMList<SMString> relOrderValue = (SMList<SMString>)groupObject.getValue().get("relationship_order");
									List<SMString> relOrder = relOrderValue.getValue();
									if (relOrder.contains(relId)) {
										relOrder.remove(relId);
										List<SMUpdate> groupUpdates = new ArrayList<SMUpdate>();
										groupUpdates.add(new SMSet("relationship_order", new SMList<SMString>(relOrder)));
										dataService.updateObject("group", groupId, groupUpdates);
									}
								}
								// remove groups from relationship's groups by user
								if (groupIdList.size() > 0) {
									dataService.removeRelatedObjects("relationship", relId, groupKey, groupIdList, false);
									groupChange = true;
								}
							}
						}
						// update type by user
						List<SMUpdate> relUpdates = new ArrayList<SMUpdate>();
						relUpdates.add(new SMSet(typeUserKey, new SMInt(type)));
						dataService.updateObject("relationship", relId, relUpdates);
						
						// if change to Accept, return friend's data for syncing
						if (type == 2L) {
							Map<String, Object> friendMap = new HashMap<String, Object>();
							friendMap.put("relationship_id", relId);
							if (userRole.equals("owner")) {
								SMString friendId = (SMString)relObject.getValue().get("invite_email");
								// if not an invite, get username instead 
								if (friendId.getValue().isEmpty()) {
									SMObject friendObject = (SMObject)relObject.getValue().get("receiver");
									friendId = (SMString)friendObject.getValue().get("username");
									friendMap.put("username", friendId);
									if (typeOther.getValue().longValue() > 2L) {
										friendMap.put("type_by_friend", new Long(2L));
									} else {
										friendMap.put("type_by_friend", typeOther.getValue());
									}
									if (typeOther.getValue().longValue() == 2L) {
										friendMap.put("action", (SMString)friendObject.getValue().get("action"));
										friendMap.put("place", (SMString)friendObject.getValue().get("place"));
										friendMap.put("status_mod_date", (SMInt)friendObject.getValue().get("status_mod_date"));
									}
								} else {
									friendMap.put("invite_email", friendId);
								}
							} else {
								SMObject friendObject = ownerObject;
								SMString friendId = (SMString)friendObject.getValue().get("username");
								friendMap.put("username", friendId);
								if (typeOther.getValue().longValue() == 2L) {
									friendMap.put("action", (SMString)friendObject.getValue().get("action"));
									friendMap.put("place", (SMString)friendObject.getValue().get("place"));
									friendMap.put("status_mod_date", (SMInt)friendObject.getValue().get("status_mod_date"));
								}
							}
							foundFriends.add(friendMap);
						} else {
							foundRelIds.add(relId);
						}
					}
				}
			}
			returnMap.put("friends", foundFriends);
			returnMap.put("changed_relationships", foundRelIds);
			returnMap.put("removed_events", removedEventIds);
			
			long currentTime = System.currentTimeMillis();
			// if there is a group change, update groups mod date (only when type is changed from friend to block or delete)
			if (groupChange) {
				List<SMUpdate> userUpdates = new ArrayList<SMUpdate>();
				userUpdates.add(new SMSet("groups_mod_date", new SMInt(currentTime)));
				dataService.updateObject("user", userId, userUpdates);
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
