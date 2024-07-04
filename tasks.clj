(ns tasks
    (:require [clojure.string :as str]
              [clojure.java.io :as io]
              [taoensso.timbre :as tlog]
              [babashka.curl :as curl]))


(def base-url "https://api.novaposhta.ua/v2.0/xml/")
(def token  (System/getenv "API_KEY"))

(tlog/debug "Api Key" token)

;;; Helper

(defn substr-count [s needle]
  (if (string? s)
    (->> s
         (re-seq (re-pattern needle))
         count)
    0))


(comment
  (string? nil)
  (substr-count nil "S"))

;;; HTTP Requests
(defn api-call [req-body]
  (let [ _ (tlog/debug "Begin request" req-body)
         {:keys [body]} (curl/post base-url {:body req-body :connect-timeout 30})]
    (tlog/trace "Response body " body)
    body))

(defn body-builder [model method & {:keys [Limit Page CityRef]}]
  (str/join ""
            ["<file>"
             (format "<modelName>%s</modelName>" (name model))
             (format "<calledMethod>%s</calledMethod>" (name method))
             "<methodProperties>"
             (when (some? Limit)
               (format "<Limit>%s</Limit>" Limit))
             (when (some? Page)
               (format "<Page>%s</Page>" Page))
             (when (string? CityRef)
               (format "<CityRef>%s</CityRef>" CityRef))
             "</methodProperties>"
             "</file>"]))

(comment
  (body-builder :test :method :Page 1 :Limit 2)
  (apply body-builder [:test :method {:Page 1 :Limit 2}]))

(defn rpc [model method & properties]
  (tlog/debug "properties" properties)
  (let [req-body (apply body-builder [model method (apply array-map properties)])] 
    (tlog/debug "Request body" req-body)
    (api-call  req-body)))

(comment 
  (rpc :AddressGeneral :getAreas))

(defn extract-items [s]
  (let [begin (str/index-of s "<data>")
        end (str/index-of s "</data>")
        items (when (and begin end)
                (subs s (+ begin 6) end))]
    items))

(defn build-export-file-path [ModelName CalledMethod]
  (format "data/%s/%s.xml" (name ModelName) (name CalledMethod)))


(defn common-import
  ([{:keys [ModelName CalledMethod] :as params}]
   (let [file (build-export-file-path ModelName CalledMethod)
         cmd-name (str ModelName "/" CalledMethod)]

     (tlog/info "Begin Import: " cmd-name)
     (tlog/info "Export file: " file)

     (with-open [writer (io/writer file)]
       (.write writer "<items>")

       (common-import params writer)

       (.write writer "</items>")
       (tlog/info "Finished import:" cmd-name))))

  ([{:keys [ModelName CalledMethod Page CityRef delay]} writer]
   (let [page (atom (or 1 Page))
         delay (or delay 1000)
         next? (atom true)
         counter (atom 0)]

     (while @next?
       (tlog/debugf "Page %s" @page)
       (let [items (-> (rpc ModelName CalledMethod :Page @page :CityRef CityRef) extract-items)
             cnt (substr-count items "<item>")]

         (when-not (nil? items)
                ;;; write raw xml from api
           (.write writer items)

                ;;; increament page
           (swap! page inc)

                ;;; sum counter
           (swap! counter + cnt))

              ;;; check if we had result, if resource is not empty, continue
         (reset! next? (pos? cnt))

              ;;; Sleep, exclude throttling 
         (tlog/debugf "Sleep %sms" delay)
         (Thread/sleep delay)))

     (tlog/infof "Imported %s " @counter))))

(comment
  (extract-items "test")
  (def response (rpc :AddressGeneral :getAreas))
  response
  (type response)
  (def items (extract-items response)) 
  items
  (rpc :AddressGeneral :getAreas :Page 1 :Limit 1)
  (rpc :AddressGeneral :getSettlements :Limit 10 :Page 2)
  )

;;; Import areas
(defn areas []
  (common-import {:ModelName :AddressGeneral 
                  :CalledMethod :getAreas}))


;;; Import Cities
(defn cities []
  (common-import {:ModelName :AddressGeneral
                  :CalledMethod :getCities :delay 20000}))

;;; Import Settelments Areas
(defn settlements-areas [] 
  (common-import {:ModelName :AddressGeneral
                  :CalledMethod :getSettlementAreas}))

;;; Import Settelments
(defn settlements []
  (common-import {:ModelName :AddressGeneral
                  :CalledMethod :getSettlements
                  :delay 10000}))

;;; Import Warehouse Types
(defn warehouse-types []
  (common-import {:ModelName :AddressGeneral
                  :CalledMethod :getWarehouseTypes}))

;;; Import Warehouses
(defn warehouses []
  (common-import {:ModelName :AddressGeneral
                  :CalledMethod :getWarehouses
                  :delay 20000}))

;;; Import streets
(defn streets []
  (let [raw-cities (slurp "data/AddressGeneral/getCities.xml")
        result (map last (re-seq #"<Ref>(.*?)<\/Ref>" raw-cities))
        ModelName :AddressGeneral
        CalledMethod :getStreet
        file (build-export-file-path ModelName CalledMethod)
        cmd-name (str ModelName "/" CalledMethod)] 
    
    (tlog/info "Begin Import: " cmd-name)
    (tlog/info "Export file: " file)
    
    (with-open [writer (io/writer file)]
      (.write writer "<items>")
    
      (doseq [CityRef result]
        (common-import {:ModelName ModelName
                        :CalledMethod CalledMethod
                        :CityRef CityRef
                        :delay 20000} writer))
    
      (.write writer "</items>")
      (tlog/info "Finished import:" cmd-name))))

(comment
  (let [raw-cities (slurp "data/AddressGeneral/getCities.xml")
        city-uuids (map last (re-seq #"<Ref>(.*?)<\/Ref>" raw-cities))]

    (prn (first city-uuids)
         (last city-uuids)))


  (or nil 10)



  (common-import {:ModelName :AddressGeneral
                  :CalledMethod :getStreet
                  :delay 20000})

  )