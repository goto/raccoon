"use strict";(self.webpackChunkfirehose=self.webpackChunkfirehose||[]).push([[651],{3905:function(e,t,n){n.d(t,{Zo:function(){return s},kt:function(){return h}});var r=n(7294);function o(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function a(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);t&&(r=r.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,r)}return n}function c(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?a(Object(n),!0).forEach((function(t){o(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):a(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function i(e,t){if(null==e)return{};var n,r,o=function(e,t){if(null==e)return{};var n,r,o={},a=Object.keys(e);for(r=0;r<a.length;r++)n=a[r],t.indexOf(n)>=0||(o[n]=e[n]);return o}(e,t);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);for(r=0;r<a.length;r++)n=a[r],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(o[n]=e[n])}return o}var u=r.createContext({}),l=function(e){var t=r.useContext(u),n=t;return e&&(n="function"==typeof e?e(t):c(c({},t),e)),n},s=function(e){var t=l(e.components);return r.createElement(u.Provider,{value:t},e.children)},p={inlineCode:"code",wrapper:function(e){var t=e.children;return r.createElement(r.Fragment,{},t)}},d=r.forwardRef((function(e,t){var n=e.components,o=e.mdxType,a=e.originalType,u=e.parentName,s=i(e,["components","mdxType","originalType","parentName"]),d=l(n),h=o,f=d["".concat(u,".").concat(h)]||d[h]||p[h]||a;return n?r.createElement(f,c(c({ref:t},s),{},{components:n})):r.createElement(f,c({ref:t},s))}));function h(e,t){var n=arguments,o=t&&t.mdxType;if("string"==typeof e||o){var a=n.length,c=new Array(a);c[0]=d;var i={};for(var u in t)hasOwnProperty.call(t,u)&&(i[u]=t[u]);i.originalType=e,i.mdxType="string"==typeof e?e:o,c[1]=i;for(var l=2;l<a;l++)c[l]=n[l];return r.createElement.apply(null,c)}return r.createElement.apply(null,n)}d.displayName="MDXCreateElement"},2257:function(e,t,n){n.r(t),n.d(t,{assets:function(){return s},contentTitle:function(){return u},default:function(){return h},frontMatter:function(){return i},metadata:function(){return l},toc:function(){return p}});var r=n(7462),o=n(3366),a=(n(7294),n(3905)),c=["components"],i={},u="Quickstart",l={unversionedId:"quickstart",id:"quickstart",title:"Quickstart",description:"This document will guide you to get Raccoon along with Kafka setup running locally. This document assumes that you have installed Docker and Kafka with host.docker.internal advertised on your machine.",source:"@site/docs/quickstart.md",sourceDirName:".",slug:"/quickstart",permalink:"/raccoon/quickstart",draft:!1,editUrl:"https://github.com/odpf/raccoon/edit/master/docs/docs/quickstart.md",tags:[],version:"current",frontMatter:{},sidebar:"docsSidebar",previous:{title:"Introduction",permalink:"/raccoon/"},next:{title:"Roadmap",permalink:"/raccoon/roadmap"}},s={},p=[{value:"Run Raccoon With Docker",id:"run-raccoon-with-docker",level:2},{value:"Publishing Your First Event",id:"publishing-your-first-event",level:2},{value:"Where To Go Next",id:"where-to-go-next",level:2}],d={toc:p};function h(e){var t=e.components,n=(0,o.Z)(e,c);return(0,a.kt)("wrapper",(0,r.Z)({},d,n,{components:t,mdxType:"MDXLayout"}),(0,a.kt)("h1",{id:"quickstart"},"Quickstart"),(0,a.kt)("p",null,"This document will guide you to get Raccoon along with Kafka setup running locally. This document assumes that you have installed Docker and Kafka with ",(0,a.kt)("inlineCode",{parentName:"p"},"host.docker.internal")," ",(0,a.kt)("a",{parentName:"p",href:"https://www.confluent.io/blog/kafka-listeners-explained/"},"advertised")," on your machine."),(0,a.kt)("h2",{id:"run-raccoon-with-docker"},"Run Raccoon With Docker"),(0,a.kt)("p",null,"Run the following command. Make sure to set ",(0,a.kt)("inlineCode",{parentName:"p"},"PUBLISHER_KAFKA_CLIENT_BOOTSTRAP_SERVERS")," according to your local Kafka setup."),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-bash"},"$ docker run -p 8080:8080 \\\n  -e SERVER_WEBSOCKET_CONN_ID_HEADER=X-User-ID \\\n  -e PUBLISHER_KAFKA_CLIENT_BOOTSTRAP_SERVERS=host.docker.internal:9092 \\\n  -e EVENT_DISTRIBUTION_PUBLISHER_PATTERN=clickstream-log \\\n  odpf/raccoon:latest\n")),(0,a.kt)("p",null,"To test whether the service is running or not, you can try to ping the server."),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-bash"},"$ curl http://localhost:8080/ping\n")),(0,a.kt)("h2",{id:"publishing-your-first-event"},"Publishing Your First Event"),(0,a.kt)("p",null,"Currently, Raccoon doesn't come with a library client. To start publishing events to Raccoon, we provide you an ",(0,a.kt)("a",{parentName:"p",href:"https://github.com/odpf/raccoon/tree/main/docs/example"},"example of a go client")," that you can refer to. You can also run the example right away if you have Go installed on your machine."),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-bash"},"# `cd` on the client example directory and run the following\n$ go run main.go sample.pb.go\n")),(0,a.kt)("p",null,"To verify the event published by Raccoon. First, you need to start a Kafka listener."),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-bash"},"$ kafka-console-consumer --bootstrap-server localhost:9092 --topic clickstream-log\n")),(0,a.kt)("h2",{id:"where-to-go-next"},"Where To Go Next"),(0,a.kt)("p",null,"For more detail about publishing events to Raccoon, you can read the ",(0,a.kt)("a",{parentName:"p",href:"https://odpf.gitbook.io/raccoon/guides/publishing"},"detailed document")," under the guides section. To understand more about how Raccoon work, you can go to the ",(0,a.kt)("a",{parentName:"p",href:"https://odpf.gitbook.io/raccoon/concepts/architecture"},"architecture document"),"."))}h.isMDXComponent=!0}}]);