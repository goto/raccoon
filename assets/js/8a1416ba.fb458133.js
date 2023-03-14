"use strict";(self.webpackChunkfirehose=self.webpackChunkfirehose||[]).push([[886],{3905:function(e,t,n){n.d(t,{Zo:function(){return p},kt:function(){return u}});var a=n(7294);function o(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function r(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);t&&(a=a.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,a)}return n}function i(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?r(Object(n),!0).forEach((function(t){o(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):r(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function s(e,t){if(null==e)return{};var n,a,o=function(e,t){if(null==e)return{};var n,a,o={},r=Object.keys(e);for(a=0;a<r.length;a++)n=r[a],t.indexOf(n)>=0||(o[n]=e[n]);return o}(e,t);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);for(a=0;a<r.length;a++)n=r[a],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(o[n]=e[n])}return o}var l=a.createContext({}),c=function(e){var t=a.useContext(l),n=t;return e&&(n="function"==typeof e?e(t):i(i({},t),e)),n},p=function(e){var t=c(e.components);return a.createElement(l.Provider,{value:t},e.children)},d={inlineCode:"code",wrapper:function(e){var t=e.children;return a.createElement(a.Fragment,{},t)}},h=a.forwardRef((function(e,t){var n=e.components,o=e.mdxType,r=e.originalType,l=e.parentName,p=s(e,["components","mdxType","originalType","parentName"]),h=c(n),u=o,m=h["".concat(l,".").concat(u)]||h[u]||d[u]||r;return n?a.createElement(m,i(i({ref:t},p),{},{components:n})):a.createElement(m,i({ref:t},p))}));function u(e,t){var n=arguments,o=t&&t.mdxType;if("string"==typeof e||o){var r=n.length,i=new Array(r);i[0]=h;var s={};for(var l in t)hasOwnProperty.call(t,l)&&(s[l]=t[l]);s.originalType=e,s.mdxType="string"==typeof e?e:o,i[1]=s;for(var c=2;c<r;c++)i[c]=n[c];return a.createElement.apply(null,i)}return a.createElement.apply(null,n)}h.displayName="MDXCreateElement"},4730:function(e,t,n){n.r(t),n.d(t,{assets:function(){return p},contentTitle:function(){return l},default:function(){return u},frontMatter:function(){return s},metadata:function(){return c},toc:function(){return d}});var a=n(7462),o=n(3366),r=(n(7294),n(3905)),i=["components"],s={},l="Architecture",c={unversionedId:"concepts/architecture",id:"concepts/architecture",title:"Architecture",description:"Raccoon written in GO is a high throughput, low-latency service that provides an API to ingest streaming data from mobile apps, sites and publish it to Kafka. Raccoon supports websockets, REST and gRPC protocols for clients to send events. With wesockets it provides long persistent connections, with no overhead of additional headers sizes as in http protocol. Racoon supports protocol buffers and JSON as serialization formats. Websockets and REST API support both whereas with gRPC only protocol buffers are supported. It provides an event type agnostic API that accepts a batch \\(array\\) of events in protobuf format. Refer here for data definitions format that Raccoon accepts.",source:"@site/docs/concepts/architecture.md",sourceDirName:"concepts",slug:"/concepts/architecture",permalink:"/raccoon/concepts/architecture",draft:!1,editUrl:"https://github.com/goto/raccoon/edit/master/docs/docs/concepts/architecture.md",tags:[],version:"current",frontMatter:{},sidebar:"docsSidebar",previous:{title:"Troubleshooting",permalink:"/raccoon/guides/troubleshooting"},next:{title:"Structure",permalink:"/raccoon/concepts/structure"}},p={},d=[{value:"System Design",id:"system-design",level:2},{value:"Connections",id:"connections",level:2},{value:"Websockets",id:"websockets",level:3},{value:"REST",id:"rest",level:3},{value:"gRPC",id:"grpc",level:3},{value:"Event Delivery Gurantee (at-least-once for most time)",id:"event-delivery-gurantee-at-least-once-for-most-time",level:3},{value:"Acknowledging events",id:"acknowledging-events",level:2},{value:"EVENT_ACK = 0",id:"event_ack--0",level:3},{value:"EVENT_ACK = 1",id:"event_ack--1",level:3},{value:"Supported Protocols and Data formats",id:"supported-protocols-and-data-formats",level:2},{value:"Request and Response Models",id:"request-and-response-models",level:2},{value:"Protobufs",id:"protobufs",level:3},{value:"JSON",id:"json",level:3},{value:"Event Distribution",id:"event-distribution",level:3},{value:"Event Deserialization",id:"event-deserialization",level:3},{value:"Channels",id:"channels",level:3},{value:"Keeping connections alive",id:"keeping-connections-alive",level:3},{value:"Components",id:"components",level:2},{value:"Kafka producer",id:"kafka-producer",level:3},{value:"Observability Stack",id:"observability-stack",level:3}],h={toc:d};function u(e){var t=e.components,s=(0,o.Z)(e,i);return(0,r.kt)("wrapper",(0,a.Z)({},h,s,{components:t,mdxType:"MDXLayout"}),(0,r.kt)("h1",{id:"architecture"},"Architecture"),(0,r.kt)("p",null,"Raccoon written in ",(0,r.kt)("a",{parentName:"p",href:"https://github.com/golang"},"GO")," is a high throughput, low-latency service that provides an API to ingest streaming data from mobile apps, sites and publish it to Kafka. Raccoon supports websockets, REST and gRPC protocols for clients to send events. With wesockets it provides long persistent connections, with no overhead of additional headers sizes as in http protocol. Racoon supports protocol buffers and JSON as serialization formats. Websockets and REST API support both whereas with gRPC only protocol buffers are supported. It provides an event type agnostic API that accepts a batch ","(","array",")"," of events in protobuf format. Refer ",(0,r.kt)("a",{parentName:"p",href:"https://goto.gitbook.io/raccoon/guides/publishing#data-formatters"},"here")," for data definitions format that Raccoon accepts."),(0,r.kt)("p",null,"Raccoon was built with a primary purpose to source or collect user behaviour data in near-real time. User behaviour data is a stream of events that occur when users traverse through a mobile app or website. Raccoon powers analytics systems, big data pipelines and other disparate consumers by providing high volume, high throughput ingestion APIs consuming real time data. Raccoon\u2019s key architecture principle is a realization of an event agnostic backend ","(","accepts events of any type without the type awareness",")",". It is this capability that enables Raccoon to evolve into a strong player in the ingestion/collector ecosystem that has real time streaming/analytical needs."),(0,r.kt)("h2",{id:"system-design"},"System Design"),(0,r.kt)("p",null,(0,r.kt)("img",{alt:"HLD",src:n(3952).Z,width:"2838",height:"1040"})),(0,r.kt)("p",null,"At a high level, the following sequence details the architecture."),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"Raccoon accepts events through one of the supported protocols."),(0,r.kt)("li",{parentName:"ul"},"The events are deserialized using the correct deserializer and then forwarded to the buffered channel."),(0,r.kt)("li",{parentName:"ul"},"A pool of worker go routines works off the buffered channel"),(0,r.kt)("li",{parentName:"ul"},"Each worker iterates over the events' batch, determines the topic based on the type and serializes the bytes to the Kafka producer synchronously.")),(0,r.kt)("p",null,"Note: The internals of each of the components like channel size, buffer sizes, publisher properties etc., are configurable enabling Raccoon to be provisioned according to the system/event characteristics and load."),(0,r.kt)("h2",{id:"connections"},"Connections"),(0,r.kt)("h3",{id:"websockets"},"Websockets"),(0,r.kt)("p",null,"Raccoon supports long-running persistent WebSocket connections with the client. Once a client makes an HTTP request with a WebSocket upgrade header, raccoon upgrades the HTTP request to a WebSocket connection end of which a persistent connection is established with the client."),(0,r.kt)("p",null,"The following sequence outlines the connection handling by Raccoon:"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"Clients make websocket connections to Raccoon by performing a http GET API call, with headers to upgrade to websocket."),(0,r.kt)("li",{parentName:"ul"},"Raccoon uses ",(0,r.kt)("a",{parentName:"li",href:"https://github.com/gorilla/websocket"},"gorilla websocket")," handlers and for each websocket connection the handlers spawn a goroutine to handle incoming requests."),(0,r.kt)("li",{parentName:"ul"},"After the websocket connection has been established, clients can send the events."),(0,r.kt)("li",{parentName:"ul"},"Construct connection identifier from the request header. The identifier is constructed from the value of ",(0,r.kt)("inlineCode",{parentName:"li"},"SERVER_WEBSOCKET_CONN_ID_HEADER")," header. For example, Raccoon is configured with ",(0,r.kt)("inlineCode",{parentName:"li"},"SERVER_WEBSOCKET_CONN_ID_HEADER=X-User-ID"),". Raccoon will check the value of X-User-ID header and make it an identifier. Raccoon then uses this identifier to check if there is already an existing connection with the same identifier. If the same connection already exists, Raccoon will disconnect the connection with an appropriate error message as a response proto.",(0,r.kt)("ul",{parentName:"li"},(0,r.kt)("li",{parentName:"ul"},"Optionally, you can also configure ",(0,r.kt)("inlineCode",{parentName:"li"},"SERVER_WEBSOCKET_CONN_GROUP_HEADER")," to support multi-tenancy. For example, you want to use an instance of Raccoon with multiple mobile clients. You can configure raccoon with ",(0,r.kt)("inlineCode",{parentName:"li"},"SERVER_WEBSOCKET_CONN_GROUP_HEADER=X-Mobile-Client"),". Then, Raccoon will use the value of X-Mobile-Client along with X-User-ID as identifier. The uniqueness becomes the combination of X-User-ID value with X-Mobile-Client value. This way, Raccoon can maintain the same X-User-ID within different X-Mobile-Client."))),(0,r.kt)("li",{parentName:"ul"},"Verify if the total connections have reached the configured limit based on ",(0,r.kt)("inlineCode",{parentName:"li"},"SERVER_WEBSOCKET_MAX_CONN")," configuration. On reaching the max connections, Raccoon disconnects the connection with an appropriate error message as a response proto."),(0,r.kt)("li",{parentName:"ul"},"Upgrade the connection and persist the identifier."),(0,r.kt)("li",{parentName:"ul"},"Add ping/pong handlers on this connection, read timeout deadline. More about these handlers in the following sections"),(0,r.kt)("li",{parentName:"ul"},"At this point, the connection is completely upgraded and Raccoon is ready to accept SendEventRequest. The handler handles each SendEventRequest by sending it to the events-channel. The events can be published by the publisher either synchronously or asynchronous based on the configuration."),(0,r.kt)("li",{parentName:"ul"},"When the connection is closed. Raccoon clean up the connection along with the identifier. The same identifier then can be reused on the upcoming connection.")),(0,r.kt)("h3",{id:"rest"},"REST"),(0,r.kt)("p",null,"Client connects to the server with the same endpoint but with POST HTTP method. As it is a rest endpoint each request is uniquely handled."),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"Connection identifier is constructed from the values of ",(0,r.kt)("inlineCode",{parentName:"li"},"SERVER_WEBSOCKET_CONN_ID_HEADER")," and ",(0,r.kt)("inlineCode",{parentName:"li"},"SERVER_WEBSOCKET_CONN_GROUP_HEADER")," header here too.")),(0,r.kt)("h3",{id:"grpc"},"gRPC"),(0,r.kt)("p",null,"It is recommended to generate the gRPC client for Raccoon's ",(0,r.kt)("a",{parentName:"p",href:"https://github.com/goto/proton/blob/main/goto/raccoon/EventService.proto"},"EventService")," and use that client to do gRPC request. Currently only unary requests are supported."),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"Client's ",(0,r.kt)("inlineCode",{parentName:"li"},"SendEvent")," method is called to send the event."),(0,r.kt)("li",{parentName:"ul"},"Connection identifier is constructed from the values of ",(0,r.kt)("inlineCode",{parentName:"li"},"SERVER_WEBSOCKET_CONN_ID_HEADER")," and ",(0,r.kt)("inlineCode",{parentName:"li"},"SERVER_WEBSOCKET_CONN_GROUP_HEADER")," in gRPC metadata.")),(0,r.kt)("p",null,"Clients can send the request anytime as long as the websocket connection is alive whereas with REST and gRPC requests can be sent only once."),(0,r.kt)("h3",{id:"event-delivery-gurantee-at-least-once-for-most-time"},"Event Delivery Gurantee ","(","at-least-once for most time",")"),(0,r.kt)("p",null,"The server for the most times provide at-least-once event delivery gurantee."),(0,r.kt)("p",null,"Event data loss happens in the following scenarios:"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("p",{parentName:"li"},"When the server shutsdown, events in-flight in the kafka buffer or those stored in the internal channels are potentially lost. The server performs, on a best-effort basis, sending all the events to kafka within a configured shutdown time ",(0,r.kt)("inlineCode",{parentName:"p"},"WORKER_BUFFER_FLUSH_TIMEOUT_MS"),". The default time is set to 5000 ms within which it is expected that all the events are sent by then.")),(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("p",{parentName:"li"},"When the upstream kafka cluster is facing a downtime"),(0,r.kt)("p",{parentName:"li"},"Every event sent from the client is stored in-memory in the buffered channels ","(","explained in the ",(0,r.kt)("inlineCode",{parentName:"p"},"Acknowledging events")," section",")",". The workers pull the events from this channel and publishes to kafka. The server does not maintain any event peristence. This is a conscious decision to enable a simpler, performant ingestion design for the server. The buffer/retries of failed events is relied upon Kafka's internal buffer/retries respectively. In future: Server can be augmented for zero-data loss or at-least-once guarantees through intermediate event persitence."))),(0,r.kt)("h2",{id:"acknowledging-events"},"Acknowledging events"),(0,r.kt)("p",null,"Event acknowledgements was designed to signify if the events batch is received and sent to Kafka successfully. This will enable the clients to retry on failed event delivery. Raccoon chooses when to send event acknowledgement based on the configuration parameter ",(0,r.kt)("inlineCode",{parentName:"p"},"EVENT_ACK"),".  "),(0,r.kt)("h3",{id:"event_ack--0"},"EVENT_ACK = 0"),(0,r.kt)("p",null,"Raccoon sends the acknowledgments as soon as it receives and deserializes the events successfully using the proto ",(0,r.kt)("inlineCode",{parentName:"p"},"SendEventRequest"),". This configuration is recommended when low latency takes precedence over end to end acknowledgement. The acks are sent even before it is produced to Kafka. The following picture depicts the sequence of the event ack."),(0,r.kt)("p",null,(0,r.kt)("img",{src:n(4597).Z,width:"4288",height:"423"})),(0,r.kt)("p",null,"Pros:"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"Performant as it does not wait for kafka/network round trip for each batch of events.")),(0,r.kt)("p",null,"Cons:"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"Potential data-loss and the clients do not get a chance to retry/resend the events. The possiblity of data-loss occurs when the kafka borker cluster is facing a downtime.")),(0,r.kt)("h3",{id:"event_ack--1"},"EVENT_ACK = 1"),(0,r.kt)("p",null,"Raccoon sends the acknowledgments after the events are acknowledged successfully from the Kafka brokers. This configuration is recommended when reliable end-to-end acknowledgements are required. Here the underlying publisher acknowledgement is leveraged."),(0,r.kt)("p",null,(0,r.kt)("img",{src:n(11).Z,width:"4290",height:"423"})),(0,r.kt)("p",null,"Pros: "),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"Minimal data loss, clients can retry/resend events in case of downtime/broker failures.")),(0,r.kt)("p",null,"Cons:"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"Increased end to end latency as clients need to wait for the event to be published.")),(0,r.kt)("p",null,"Considering that kafka is set up in a clustered, cross-region, cross-zone environment, the chances of it going down are mostly unlikely. In case if it does, the amount of events lost is negligible considering it is a streaming system and is expected to forward millions of events/sec."),(0,r.kt)("p",null,"When an SendEventRequest is sent to Raccoon over any connection be it Websocket/HTTP/gRPC a corresponding response is sent by the server inidcating whether the event was consumed successfully or not."),(0,r.kt)("h2",{id:"supported-protocols-and-data-formats"},"Supported Protocols and Data formats"),(0,r.kt)("table",null,(0,r.kt)("thead",{parentName:"table"},(0,r.kt)("tr",{parentName:"thead"},(0,r.kt)("th",{parentName:"tr",align:"center"},"Protocol"),(0,r.kt)("th",{parentName:"tr",align:"center"},"Data Format"),(0,r.kt)("th",{parentName:"tr",align:"center"},"Version"))),(0,r.kt)("tbody",{parentName:"table"},(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:"center"},"WebSocket"),(0,r.kt)("td",{parentName:"tr",align:"center"},"Protobufs"),(0,r.kt)("td",{parentName:"tr",align:"center"},"v0.1.0")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:"center"},"WebSocket"),(0,r.kt)("td",{parentName:"tr",align:"center"},"JSON"),(0,r.kt)("td",{parentName:"tr",align:"center"},"v0.1.2")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:"center"},"REST API"),(0,r.kt)("td",{parentName:"tr",align:"center"},"JSON"),(0,r.kt)("td",{parentName:"tr",align:"center"},"v0.1.2")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:"center"},"REST API"),(0,r.kt)("td",{parentName:"tr",align:"center"},"Protobufs"),(0,r.kt)("td",{parentName:"tr",align:"center"},"v0.1.2")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:"center"},"gRPC"),(0,r.kt)("td",{parentName:"tr",align:"center"},"Protobufs"),(0,r.kt)("td",{parentName:"tr",align:"center"},"v0.1.2")))),(0,r.kt)("h2",{id:"request-and-response-models"},"Request and Response Models"),(0,r.kt)("h3",{id:"protobufs"},"Protobufs"),(0,r.kt)("p",null,"When an ",(0,r.kt)("a",{parentName:"p",href:"https://github.com/goto/proton/blob/main/goto/raccoon/v1beta1/raccoon.proto"},"SendEventRequest")," proto below containing events are sent over the wire"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-text"},"message SendEventRequest {\n  //unique guid generated by the client for this request\n  string req_guid = 1;\n  // time probably when the client sent it\n  google.protobuf.Timestamp sent_time = 2;\n  // actual events\n  repeated Event events = 3;\n}\n")),(0,r.kt)("p",null,"a corresponding ",(0,r.kt)("a",{parentName:"p",href:"https://github.com/goto/proton/blob/main/goto/raccoon/v1beta1/raccoon.proto"},"SendEventResponse")," is sent by the server."),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-text"},"message SendEventResponse {\n  Status status = 1;\n  Code code = 2;\n      /* time when the response is generated */\n  int64 sent_time = 3;\n      /* failure reasons if any */\n  string reason = 4;\n      /* Usually detailing the success/failures */\n  map<string, string> data = 5;\n}\n")),(0,r.kt)("h3",{id:"json"},"JSON"),(0,r.kt)("p",null,"When a JSON event like the one metoined below is sent a corresponding JSON response is sent by the server."),(0,r.kt)("p",null,(0,r.kt)("strong",{parentName:"p"},"Request")),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-json"},'{\n  "req_guid": "1234abcd",\n  "sent_time": {\n    "seconds": 1638154927,\n    "nanos": 376499000\n  },\n  "events": [\n    {\n      "eventBytes": "Cg4KCHNlcnZpY2UxEgJBMRACIAEyiQEKJDczZTU3ZDlhLTAzMjQtNDI3Yy1hYTc5LWE4MzJjMWZkY2U5ZiISCcix9QzhsChAEekGEi1cMlNAKgwKAmlkEgJpZBjazUsyFwoDaU9zEgQxMi4zGgVBcHBsZSIDaTEwOiYKJDczZTU3ZDlhLTAzMjQtNDI3Yy1hYTc5LWE4MzJjMWZkY2U5Zg==",\n      "type": "booking"\n    }\n  ]\n}\n')),(0,r.kt)("p",null,(0,r.kt)("strong",{parentName:"p"},"Response")),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-json"},'{\n  "status": 1,\n  "code": 1,\n  "sent_time": 1638155915,\n  "data": {\n    "req_guid": "1234abcd"\n  }\n}\n')),(0,r.kt)("h3",{id:"event-distribution"},"Event Distribution"),(0,r.kt)("p",null,"Event distribution works by finding the type for each event in the batch and sending them to appropriate kafka topic. The topic name is determined by the following code"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-text"},"topic := fmt.Sprintf(pr.topicFormat, event.Type)\n")),(0,r.kt)("p",null,"where ",(0,r.kt)("strong",{parentName:"p"},"topicformat")," - is the configured pattern ",(0,r.kt)("inlineCode",{parentName:"p"},"EVENT_DISTRIBUTION_PUBLISHER_PATTERN")," ",(0,r.kt)("strong",{parentName:"p"},"type")," - is the type set by the client when the event proto is generated"),(0,r.kt)("p",null,"For eg. setting the"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-text"},"EVENT_DISTRIBUTION_PUBLISHER_PATTERN=topic-%s-log\n")),(0,r.kt)("p",null,"and a type such as ",(0,r.kt)("inlineCode",{parentName:"p"},"type=viewed")," in the ",(0,r.kt)("a",{parentName:"p",href:"https://github.com/goto/proton/blob/main/goto/raccoon/Event.proto"},"event")," format"),(0,r.kt)("p",null,"and a type such as ",(0,r.kt)("inlineCode",{parentName:"p"},"type=viewed")," in the event format"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-text"},"message Event {\n  /*\n  `eventBytes` is where you put bytes serialized event.\n  */\n  bytes eventBytes = 1;\n  /*\n  `type` denotes an event type that the producer of this proto message may set.\n  It is currently used by raccoon to distribute events to respective Kafka topics. However the\n  users of this proto can use this type to set strings which can be processed in their\n  ingestion systems to distribute or perform other functions.\n  */\n  string type = 2;\n }\n")),(0,r.kt)("p",null,"will have the event sent to a topic like"),(0,r.kt)("p",null,(0,r.kt)("inlineCode",{parentName:"p"},"topic-viewed-log")),(0,r.kt)("p",null,"The event distribution does not depend on any partition logic. So events can be randomnly distrbuted to any kafka partition."),(0,r.kt)("h3",{id:"event-deserialization"},"Event Deserialization"),(0,r.kt)("p",null,"The top level wrapper ",(0,r.kt)("inlineCode",{parentName:"p"},"SendEventRequest")," is deserialized which provides a list of events of type ",(0,r.kt)("inlineCode",{parentName:"p"},"Event")," proto. This event wrapper composes of serialized bytes, which is the actual event, set in the field ",(0,r.kt)("inlineCode",{parentName:"p"},"bytes")," inside the ",(0,r.kt)("inlineCode",{parentName:"p"},"Event")," proto. Raccoon does not open this underlying bytes. The deserialization is used to unwrap the event type and determine the topic that the ",(0,r.kt)("inlineCode",{parentName:"p"},"eventBytes")," ","(","an event",")"," need to be sent to."),(0,r.kt)("h3",{id:"channels"},"Channels"),(0,r.kt)("p",null,"Buffered Channels are used to store the incoming events' batch. The channel sizes can be configured based on the load & capacity."),(0,r.kt)("h3",{id:"keeping-connections-alive"},"Keeping connections alive"),(0,r.kt)("p",null,"The server ensures that the connections are recyclable. It adopts mechanisms to check connection time idleness. The handlers ping clients very 30 seconds ","(","configurable",")",". If the client does not respond within a stipulated time the connection is marked as corrupt. Every subsequent read/write message there after on this connection fails. Raccoon removes the connections post this. Clients can also ping the server while the server responds with pongs to these pings. Clients can programmtically reconnect on failed or corrupt server connections."),(0,r.kt)("h2",{id:"components"},"Components"),(0,r.kt)("h3",{id:"kafka-producer"},"Kafka producer"),(0,r.kt)("p",null,"Raccoon uses ",(0,r.kt)("a",{parentName:"p",href:"https://github.com/confluentinc/confluent-kafka-go"},"confluent go kafka")," as the producer client to publish events. Publishing events are light weight and relies on kafka producer's retries. Confluent internally uses librdkafka which produces events asynchronously. Application writes messages using a functional based producer API"),(0,r.kt)("p",null,(0,r.kt)("inlineCode",{parentName:"p"},"Produce(message, deliveryChannel)")," -- ",(0,r.kt)("inlineCode",{parentName:"p"},"deliveryChannel")," is where the delivery reports or acknowledgements are received."),(0,r.kt)("p",null,"Raccoon internally checks for these delivery reports before pulling the next batch of events. On failed deliveries the appropriate metrics are updated. This mechanism makes the events delivery synchronous and a reliable events delivery."),(0,r.kt)("h3",{id:"observability-stack"},"Observability Stack"),(0,r.kt)("p",null,"Raccoon internally uses ",(0,r.kt)("a",{parentName:"p",href:"https://gopkg.in/alexcesaro/statsd.v2"},"statsd")," go module client to export metrics in StatsD line protocol format. A recommended choice for observability stack would be to host ",(0,r.kt)("a",{parentName:"p",href:"https://www.influxdata.com/time-series-platform/telegraf/"},"telegraf")," as the receiver of these measurements and expoert it to ",(0,r.kt)("a",{parentName:"p",href:"https://www.influxdata.com/get-influxdb/"},"influx"),", influx to store the metrics, ",(0,r.kt)("a",{parentName:"p",href:"https://grafana.com/"},"grafana")," to build dashboards using Influx as the source."))}u.isMDXComponent=!0},11:function(e,t,n){t.Z=n.p+"assets/images/raccoon_async-293af9c9ceab1777e1780db272c51fa5.png"},3952:function(e,t,n){t.Z=n.p+"assets/images/raccoon_hld-6627a7d982b7dd633d7214a85eacb31e.png"},4597:function(e,t,n){t.Z=n.p+"assets/images/raccoon_sync-3682a8af8546dae4d1ed0f7026fc1f60.png"}}]);