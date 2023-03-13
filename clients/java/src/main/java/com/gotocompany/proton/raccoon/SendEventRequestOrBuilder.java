// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: .temp/odpf/raccoon/v1beta1/raccoon.proto

package com.gotocompany.proton.raccoon;

public interface SendEventRequestOrBuilder extends
    // @@protoc_insertion_point(interface_extends:odpf.raccoon.v1beta1.SendEventRequest)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <pre>
   * `req_guid` is unique identifier of the request the client is making.
   * Raccoon uses the identifier to send response of the request. The client can handle the
   * response accordingly. For example, the client can retry the request in case the response is
   * giving `INTERNAL_ERROR` code with "publisher failed" reason.
   * This identifier is necessary because on event-based protocols like WebSocket the response is
   * returned asynchronously. If there is no identifier, no way the client can tell which response
   * belongs to which request.
   * Apart from sending response, `req_guid` is used to log some informations on 'debug' level. You can search the 
   * debug logs with `ReqGUID` keyword.
   * </pre>
   *
   * <code>string req_guid = 1 [json_name = "reqGuid"];</code>
   * @return The reqGuid.
   */
  java.lang.String getReqGuid();
  /**
   * <pre>
   * `req_guid` is unique identifier of the request the client is making.
   * Raccoon uses the identifier to send response of the request. The client can handle the
   * response accordingly. For example, the client can retry the request in case the response is
   * giving `INTERNAL_ERROR` code with "publisher failed" reason.
   * This identifier is necessary because on event-based protocols like WebSocket the response is
   * returned asynchronously. If there is no identifier, no way the client can tell which response
   * belongs to which request.
   * Apart from sending response, `req_guid` is used to log some informations on 'debug' level. You can search the 
   * debug logs with `ReqGUID` keyword.
   * </pre>
   *
   * <code>string req_guid = 1 [json_name = "reqGuid"];</code>
   * @return The bytes for reqGuid.
   */
  com.google.protobuf.ByteString
      getReqGuidBytes();

  /**
   * <pre>
   * `sent_time` defines the time the request is sent.
   * `sent_time` is used to calculate various metrics. The main metric uses `sent_time` is duration from the
   * request is sent until the events are published.
   * </pre>
   *
   * <code>.google.protobuf.Timestamp sent_time = 2 [json_name = "sentTime"];</code>
   * @return Whether the sentTime field is set.
   */
  boolean hasSentTime();
  /**
   * <pre>
   * `sent_time` defines the time the request is sent.
   * `sent_time` is used to calculate various metrics. The main metric uses `sent_time` is duration from the
   * request is sent until the events are published.
   * </pre>
   *
   * <code>.google.protobuf.Timestamp sent_time = 2 [json_name = "sentTime"];</code>
   * @return The sentTime.
   */
  com.google.protobuf.Timestamp getSentTime();
  /**
   * <pre>
   * `sent_time` defines the time the request is sent.
   * `sent_time` is used to calculate various metrics. The main metric uses `sent_time` is duration from the
   * request is sent until the events are published.
   * </pre>
   *
   * <code>.google.protobuf.Timestamp sent_time = 2 [json_name = "sentTime"];</code>
   */
  com.google.protobuf.TimestampOrBuilder getSentTimeOrBuilder();

  /**
   * <pre>
   * `events` is where the client put all the events wrapped in `Event`.
   * As mentioned above, the request allows the client to push more than one event. Normally you want to batch
   * the events to optimize the network call. 
   * </pre>
   *
   * <code>repeated .odpf.raccoon.v1beta1.Event events = 3 [json_name = "events"];</code>
   */
  java.util.List<Event>
      getEventsList();
  /**
   * <pre>
   * `events` is where the client put all the events wrapped in `Event`.
   * As mentioned above, the request allows the client to push more than one event. Normally you want to batch
   * the events to optimize the network call. 
   * </pre>
   *
   * <code>repeated .odpf.raccoon.v1beta1.Event events = 3 [json_name = "events"];</code>
   */
  Event getEvents(int index);
  /**
   * <pre>
   * `events` is where the client put all the events wrapped in `Event`.
   * As mentioned above, the request allows the client to push more than one event. Normally you want to batch
   * the events to optimize the network call. 
   * </pre>
   *
   * <code>repeated .odpf.raccoon.v1beta1.Event events = 3 [json_name = "events"];</code>
   */
  int getEventsCount();
  /**
   * <pre>
   * `events` is where the client put all the events wrapped in `Event`.
   * As mentioned above, the request allows the client to push more than one event. Normally you want to batch
   * the events to optimize the network call. 
   * </pre>
   *
   * <code>repeated .odpf.raccoon.v1beta1.Event events = 3 [json_name = "events"];</code>
   */
  java.util.List<? extends EventOrBuilder>
      getEventsOrBuilderList();
  /**
   * <pre>
   * `events` is where the client put all the events wrapped in `Event`.
   * As mentioned above, the request allows the client to push more than one event. Normally you want to batch
   * the events to optimize the network call. 
   * </pre>
   *
   * <code>repeated .odpf.raccoon.v1beta1.Event events = 3 [json_name = "events"];</code>
   */
  EventOrBuilder getEventsOrBuilder(
      int index);
}
