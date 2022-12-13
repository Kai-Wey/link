(ns link.sample-server
  (:require  [link.threads :as link-threads]
             [link.http.http2 :as h2]
             [link.codec :as ld]
             [link.core :as lc]
             [link.tcp :as lt]
             [link.http :as lh]
             [link.ssl :as ls]
             [clojure.java.io :as io]
             [clojure.core.async :as a]
             [clojure.pprint :as pp]
             [reitit.ring :as ring]
             [muuntaja.core :as m]
             [ring.middleware.params :as params]
             [reitit.ring.middleware.muuntaja :as muuntaja]
             [reitit.coercion.spec]
             [reitit.ring.coercion :as rrc]
             [jsonista.core :as js]
             [tupelo.core :as t]
             [spec-tools.data-spec :as ds])
   (:import [java.net SocketAddress URI Proxy ProxySelector InetSocketAddress]
            [java.nio ByteBuffer]
            [io.netty.channel Channel SimpleChannelInboundHandler ChannelHandlerContext ChannelPipeline]
            [io.netty.handler.ssl.util SelfSignedCertificate]
            [io.netty.buffer ByteBuf Unpooled]
            [io.netty.util ReferenceCountUtil]
            [io.netty.handler.codec.http2 Http2SecurityUtil]
            [io.netty.handler.codec.http DefaultHttpContent DefaultHttpRequest HttpVersion HttpMethod HttpHeaders HttpServerCodec
             HttpServerUpgradeHandler HttpObjectAggregator]
            [io.netty.handler.codec.http.multipart DefaultHttpDataFactory HttpPostRequestDecoder InterfaceHttpData InterfaceHttpData$HttpDataType FileUpload]
            [java.util.concurrent ConcurrentHashMap]
            [java.net SocketAddress URI Proxy ProxySelector InetSocketAddress]
            [java.util List Date]
            [java.nio.file Files]))

(comment
  2022-12-09 15:26:17,253 WARN  io.netty.channel.AbstractChannelHandlerContext: An exception 'io.netty.handler.codec.http2.Http2Exception: invalid header name [Content-Type]' [enable DEBUG level for full stacktrace] was thrown by a user handler's exceptionCaught() method while handling the following exception:
io.netty.handler.codec.http2.Http2Exception: invalid header name [Content-Type]
        at io.netty.handler.codec.http2.Http2Exception.connectionError(Http2Exception.java:108)
        at io.netty.handler.codec.http2.DefaultHttp2Headers$2.validateName(DefaultHttp2Headers.java:67)
        at io.netty.handler.codec.http2.DefaultHttp2Headers$2.validateName(DefaultHttp2Headers.java:40)
        at io.netty.handler.codec.DefaultHeaders.set(DefaultHeaders.java:430)
        at link.http.http2$ring_response_to_http2.invokeStatic(http2.clj:107)
        at link.http.http2$ring_response_to_http2.invoke(http2.clj:86)
        at link.http.http2$handle_full_request$send_fn__6481.invoke(http2.clj:180)
  )

(defn some-post-check
  [body]
  
  #_(println "\n === Body In Check ===")
  #_(pp/pprint body)

  (if (= (:outbound body)"response")
    true
    false))

(def routes-spec
  (let [http2-routes {"/get-something" {:organisation string? (ds/opt :consumer) string?}
                      "/post-something" some-post-check}
       ]
    {"/http2-test" (merge http2-routes)}))

(def client-push-ch
  #_(when-not *compile-files*))

(defn async-handler
  ""
  [handler-ch req resp-fn raise-fn]
  #_(println "\n ====================== Called in async-handler")
  #_(pp/pprint req)

  (a/go
    (let [body-or-query (if (= :get (get req :request-method))
                          :query
                          :body)
          {{{:keys [async push response-level consumer]} body-or-query} :parameters} req
          form-response (fn [body]
                          {:requestor (get-in req [:parameters :query :requestor])
                           :instance "Http2 Test Instance"
                           :status 200
                           :body body})

          response-body (if async
                          (let [response-ch (when (not= "none" push)
                                              {:ch (get client-push-ch :in) :response-level response-level :endpoint consumer})
                                response-body "Request put for asynchronous processing."]
                            (-> handler-ch (get :in) (a/>! {:response-ch response-ch :req req}))
                            response-body)

                          (let [endpoint-ch (a/chan 1)
                                response-ch {:ch endpoint-ch :response-level response-level}]
                            (-> handler-ch (get :in) (a/>! {:response-ch response-ch :req req}))
                            (-> (a/<!! endpoint-ch)
                                (js/write-value-as-string js/keyword-keys-object-mapper))))]
      (-> response-body
          form-response
          resp-fn))))



;; === PROTOTYPE API ENDPOINT ===
(def api-endpoint-hierarchy (atom (make-hierarchy)))

;; api-1 child
(defn make-api-child
  [uri api-function]
  (swap! api-endpoint-hierarchy derive uri api-function))

;; "Map of paths to its corresponding API Endpoint"
(def path-to-api
  {:api-1 ["/http2-test/get-something"]
   :api-2 ["/http2-test/post-something"]})

;; "Mapping of paths to its corresponding API Endpoint"
(doseq [[k v] path-to-api
        x v]
  (make-api-child (keyword x) k))

(defmulti api-endpoint (fn [req _] (-> req (get :uri) keyword)) :hierarchy api-endpoint-hierarchy)


(defmethod api-endpoint :api-1
  [{{{:keys [organisation]} :query :as query-params} :parameters uri :uri :as req}
   {:keys [ch response-level] :as response-ch}]
  (a/go
   (let [response-map  {:success true :response {:msg "Success in HTTP2 Test" :status 200 :body "Some Sample Body"}}]
     (a/>! ch response-map))))

(defmethod api-endpoint :api-2
  [{{{:keys [organisation]} :query :as query-params} :parameters uri :uri :as req}
   {:keys [ch response-level] :as response-ch}]
  (a/go
    (println "\n === Full Request ===")
    (pp/pprint req)

    (println "\n === Body ===")
    #_(pp/pprint (t/json->edn (:body req)))
    (a/>! ch {:body "Thanks for posting!"})))

;; === END OF API ENDPOINT ===


(defn handler
  [{:keys [:in-size in-size]}]
  (let [in (a/chan in-size)]
    (a/go
      (loop []
        (let [{:keys [response-ch req]} (a/<! in)]
          (api-endpoint req response-ch))
        (recur)))
    {:in in}))

(def handler-ch
  (when-not *compile-files*
    (handler {:in-size 1000})))


(def route-uris
  (let [handler (partial async-handler handler-ch)
        uris [["/http2-test"
               ["/get-something" {:get {:parameters {:query (get-in routes-spec ["/http2-test" "/get-something"])} :handler handler}}]
               ["/post-something" {:post {:parameters {:body (get-in routes-spec ["/http2-test" "/post-something"])} :handler handler}}]]]]
    uris))

(defn routes
  [route-uris]
  ;(try ;remove
  (println "Ring-fn called")
  (ring/ring-handler
   (ring/router
    route-uris
    #_{:data {:muuntaja m/instance
            :coercion reitit.coercion.spec/coercion
            :middleware [rrc/coerce-exceptions-middleware
                         rrc/coerce-request-middleware rrc/coerce-response-middleware]}}
    {:data {:muuntaja m/instance
            :coercion reitit.coercion.spec/coercion
            :middleware [params/wrap-params muuntaja/format-middleware rrc/coerce-exceptions-middleware
                         rrc/coerce-request-middleware rrc/coerce-response-middleware]}})
   (ring/create-default-handler
    {:not-found (constantly {:status 404, :body "404 Not Found"})
     :method-not-allowed (constantly {:status 405, :body "405 Not Allowed"})
     :not-acceptable (constantly {:status 406, :body "406 Not Acceptable"})})))
  ;(catch Throwable t (pp/pprint t #_(Throwable->map t)))




(comment
  ;; The http 2 part
  (defn server-init
    "start a h2/h2c server depending on config"
    []
    (let [{:keys [host port threads ssl max-request-body]} {:host "0.0.0.0"
                                                            :port 40005
                                                            :threads 10
                                                            :client_whitelist [{:host-name "", :host-address ""}]
                                                            :ssl {:auth-type "None"
                                                                  :server-cert "/home/kaiwey/workspace/sample-certificates/server1cert.pem"
                                                                  :server-key "/home/kaiwey/workspace/sample-certificates/serverkey.pem"
                                                                  :client-ca-cert ""}}
        ;; do consider mutual authentication.
          ssl-context (fn []
                        (ls/ssl-context-for-http2 (io/file (:server-cert ssl)) (io/file (:server-key ssl)) :jdk))]

      (if (= "None" (get ssl :auth-type))
        (lh/h2-server port (routes route-uris) (ssl-context) :threads threads :async? true :max-request-body max-request-body)
        (lh/h2c-server port (routes route-uris) :threads threads :async? true :max-request-body max-request-body))))

  (server-init)

  ;; Tested with cURL
  ;; curl -cert /home/kaiwey/workspace/sample-certificates/client_signed_cert.pem --cacert /home/kaiwey/workspace/sample-certificates/cacert.pem -H "Content-Type: application/json" -d "@/home/kaiwey/workspace/search-and-replace-req.json" -v --http2 https://0.0.0.0:40005/http2-test/post-something

  )


(comment
  (defn long-channel-id
    ""
    [ch]
    (-> ch .id .asLongText))


  (defn on-active-handler []
    (lc/create-handler
     (lc/on-active [ch]
                   #_(println "on-active-handler"))))
  
  (defn on-event-handler []
    (lc/create-handler
     (lc/on-event [ch evt]
                  #_(println "on-event"))))

  (defn cas-channel-address
    ""
    [ch]
    (let [local (-> ch lc/channel-addr) remote (-> ch lc/remote-addr)]
      (->> (map (fn [k o] {k {:ip (-> o .getAddress .getHostAddress) :port (.getPort o) :hostname (.getHostString o)}}) ["local" "remote"] [local remote])
           (into (hash-map)))))

  (defn create-http-handler-from-async-ring [ring-fn debug?]
    (lc/create-handler
     (lc/on-inactive [ch]
                   ;(println "on-inactive-")
                     )
     (lc/on-message [ch msg]
                  ;(println "--on-message--")
                    (ReferenceCountUtil/retain msg)
                    (let [channel-id (long-channel-id ch)
                          address (cas-channel-address ch)

                          ;; using blocking timestamp
                          req (merge (lh/ring-request ch msg)
                                     {:channel-id channel-id :address address :internal false})
                        ;_ (println "on-message req keys:" (keys req))
                        ;_ (println "on-message req:" req)
                          resp-fn (fn [resp]
                                  ;(println "lc/on-message:" resp)
                                    (let [netty-resp (lh/ring-response (if-not (contains? resp :binary) resp (dissoc resp :binary)) (.alloc ^Channel ch))]

                                      (println "\n === netty-resp from create-http-handler-from-ring ===")
                                      (pp/pprint netty-resp)
                                      (lc/send! ch netty-resp)))
                                  ;(lc/send! ch (lh/ring-response resp (.alloc ^Channel ch))))
                          raise-fn (fn [error]
                                     (lh/http-on-error ch error debug?))]
                      (println "\n === req from create-http-handler-from-ring 123===")
                      (pp/pprint req)


                      (ring-fn req resp-fn raise-fn)
                      (ReferenceCountUtil/release msg)))
     (lc/on-error [ch exc]
                  (println "Uncaught exception")
                  (lh/http-on-error ch exc debug?))))

  (defn h2c-handlers [ring-fn max-length executor debug? async?]
    (let [http-server-codec (HttpServerCodec.)
          upgrade-handler (HttpServerUpgradeHandler. http-server-codec
                                                     (h2/http2-upgrade-handler ring-fn async? debug?))]
      [http-server-codec
       upgrade-handler
       (proxy [SimpleChannelInboundHandler] []
       ;--
         (channelActive [ctx]
           (let [^ChannelPipeline ppl (.pipeline ctx)
                 this-ctx (.context ppl this)]
             (.addAfter ppl executor (.name this-ctx) nil (on-active-handler)))
           (.fireChannelActive ^ChannelHandlerContext ctx))
         (userEventTriggered [^ChannelHandlerContext ctx evt]
           (let [^ChannelPipeline ppl (.pipeline ctx)
                 this-ctx (.context ppl this)]
             (.addAfter ppl executor (.name this-ctx) nil (on-event-handler)))
           (.fireUserEventTriggered ctx evt))
       ;--
         (channelRead0 [ctx msg]
           (let [^ChannelPipeline ppl (.pipeline ctx)
                 this-ctx (.context ppl this)]
             (.addAfter ppl executor (.name this-ctx) nil
                        (if async?
                          (create-http-handler-from-async-ring ring-fn debug?)
                          (lh/create-http-handler-from-ring ring-fn debug?)))
             (.replace ppl this nil (HttpObjectAggregator. max-length)))
           (.fireChannelRead ^ChannelHandlerContext ctx (ReferenceCountUtil/retain msg))))]))





  (defn h2c-server [port ring-fn
                    & {:keys [threads executor debug host
                              max-request-body async?
                              options]
                       :or {threads nil
                            executor nil
                            debug false
                            host "0.0.0.0"
                            max-request-body 1048576}}]
    (let [executor (if threads (link-threads/new-executor threads) executor)
          handler-spec (fn [_] (h2c-handlers ring-fn max-request-body executor debug async?))]
      (lt/tcp-server port handler-spec :host host :options options)))


  (defn api-server
    ""
    [routes {:keys [host port threads ssl]}]
    #_(println ssl)
    (let [{:keys [auth-type server-cert server-key client-ca-cert]} ssl
          ssl-context (case auth-type
                        "Mutual" "" #_(es/ssl-context-server-mutual-auth (es/gen-key 65537 (read-string (slurp server-key)))
                                                                         (es/gen-cert (read-string (slurp server-cert)))
                                                                         (es/gen-cert (read-string (slurp client-ca-cert)))
                                                                         :jdk)
                        "1-Way-File" (ls/ssl-context-for-http2 (io/file server-cert) (io/file server-key) :jdk)
                        "1-Way-Self-Signed" (let [self-s (SelfSignedCertificate.)]
                                              (ls/ssl-context-for-http2 (.certificate self-s) (.privateKey self-s) :jdk))
                        "None" nil)]
      (if (= "None" auth-type)
        (h2c-server port routes :threads threads :async? true)
      ;(lh/h2c-server port routes :threads threads :async? true #_false) ;library
      ;Make it async by allowing the handler to take req resp-fn raise-fn.
        (lh/h2-server port routes ssl-context :threads threads :async? true))))

  (api-server (routes route-uris) {:host "0.0.0.0"
                                   :port 40000
                                   :threads 10
                                   :client_whitelist [{:host-name "", :host-address ""}]
                                   :ssl {:auth-type "None"
                                         :server-cert ""
                                         :server-key ""
                                         :client-ca-cert ""}})
  )






