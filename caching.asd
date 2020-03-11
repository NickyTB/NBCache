;;;; caching.asd

(asdf:defsystem #:caching
  :description "Describe caching here"
  :author "Your Name <your.name@example.com>"
  :license  "Specify license here"
  :version "0.0.1"
  :serial t
  :depends-on (#:lparallel #:bordeaux-threads #:local-time)
  :components ((:file "package")
							 (:file "caching")))
